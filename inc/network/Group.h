#pragma once
#include <cstdio>
#include <string>
#include "Config.h"
#include "spdlog/spdlog.h"
#include "./MySocket/MyClient.h"

bool client_is_runing[3];
bool group_is_runing = false;
bool group_is_deinit = false;

static Package remoteGet(int32_t select_column,
          int32_t where_column, const std::string &column_key, size_t column_key_len);

static bool serverSyncInit() {
  return group_is_runing;
}

static bool serverSyncDeinit() {
  return group_is_deinit;
}

void *runServer(void *input) {
  std::string s = std::string((char *)input);
  int index = s.find(":");
  std::string ip = s.substr(0,index);
  std::string port = s.substr(index + 1, s.length());
  my_server_run(ip.c_str(), stoi(port));
}


static void initGroup(const char* host_info, const char* const* peer_host_info, size_t peer_host_info_num) {

  for (int i = 0; i < peer_host_info_num; i++) {
    client_is_runing[i] = true;
  }

  pthread_t serverId;
  int ret = pthread_create(&serverId, NULL, runServer, (void *)host_info);

  for (int i = 0; i < peer_host_info_num; i++) {
    std::string s = std::string(peer_host_info[i]);
    std::string ip, port;
    int flag = s.find(":");
    ip = s.substr(0,flag);
    port = s.substr(flag + 1, s.length());

    spdlog::info("Connect Server {}, ip: {}, port: {}", i, ip, port);
    for (int tid = 0; tid < 50; tid++) {
      while (true) {
        if (create_connect(ip.c_str(), stoi(port), tid, i) < 0) {
          std::this_thread::sleep_for(std::chrono::seconds(1));
        } else {
          break;
        }
      }
    }
    group_is_runing = true;
  }

  for (int i = 0; i < peer_host_info_num; i++) {
    while (true) {
      if (client_sync(4, i, 0) > 0) {
        spdlog::info("Server {} init Success", i);
        break;
      } else {
        spdlog::info("Server {} init time out", i);
      }
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }
  std::this_thread::sleep_for(std::chrono::seconds(15));
}

 // 指数退避
const uint32_t retry_base_interval = 10; //单位毫秒
const uint32_t max_retry_times = 3;

static Package clientRemoteGet(int32_t select_column,
          int32_t where_column, const void *column_key, size_t column_key_len, int tid) {
  Package result;
  for (int i = 0; i < 3; i++) {
    int retry_time = 0;
    while (true) { // backoff
      if (!client_is_runing[i]) break;
      if (retry_time > max_retry_times) { // 超过10次放弃retry，可能无法获得所有数据
        spdlog::error("network congestion!!!");
        client_is_runing[i] = false;
        break;
      } else if (retry_time != 0) {
        std::this_thread::sleep_for(std::chrono::milliseconds(retry_base_interval << (retry_time - 1)));
      }
      // 杨樊：我们这边有个重要原则是：读取不会涉及已kill节点
      spdlog::debug("Begin Remote Get Select {}, where: {}, from {}", select_column, where_column, i);
      Package package = client_send(select_column, where_column, column_key, column_key_len, tid, i);
      if (package.size == -1) {
        retry_time++;
        continue;
      }
      memcpy(result.data + result.size, package.data, package.size);
      result.size += package.size;
      break;
    }
  }
  return result;
}

static void deInitGroup() {
  group_is_deinit = true;
  for (int i = 0; i < PeerHostInfoNum; i++) {
    while (true) {
      if (client_sync(5, i, 0) > 0) {
        spdlog::info("Server {} ready to deinit", i);
        break;
      } else {
        spdlog::info("Server {} not ready to deinit", i);
      }
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }
  std::this_thread::sleep_for(std::chrono::seconds(15));
}