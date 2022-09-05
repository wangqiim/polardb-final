#pragma once
#include <cstdio>
#include <string>
#include <atomic>
#include "Config.h"
#include "spdlog/spdlog.h"
#include "./MySocket/MyClient.h"

std::string global_peer_host_info[PeerHostInfoNum];

std::atomic<bool> group_is_runing = {false};
std::atomic<bool> group_is_deinit = {false};
std::atomic<bool> client_is_running[PeerHostInfoNum];

static std::atomic<uint32_t> pk_remote_count(0);
static std::atomic<uint32_t> uk_remote_count(0);
static std::atomic<uint32_t> sk_remote_count(0);

static bool serverSyncInit() {
  return group_is_runing.load();
}

static bool serverSyncDeinit() {
  spdlog::debug("[serverSyncDeinit] group_is_deinit = {}", group_is_deinit.load());
  return group_is_deinit.load();
}

void *runServer(void *input) {
  std::string s = std::string((char *)input);
  int index = s.find(":");
  std::string ip = s.substr(0,index);
  std::string port = s.substr(index + 1, s.length());
  my_server_run(ip.c_str(), stoi(port));
}

// 该函数只会被超出50的tid调用一次
// must success, without retry
static void mustAddConnect(int tid) {
  for (int i = 0; i < PeerHostInfoNum; i++) {
    std::string s = global_peer_host_info[i];
    std::string ip, port;
    int flag = s.find(":");
    ip = s.substr(0,flag);
    port = s.substr(flag + 1, s.length());
    if (create_connect(ip.c_str(), stoi(port), tid, i) < 0) { // 可能节点被kill掉了，不用重试
      spdlog::warn("[mustAddConnect] tid: {} add connect fail, errno = {}", tid, errno);
    } else {
      spdlog::debug("[mustAddConnect] tid: {} add connect");
    }
  }
}

static void initGroup(const char* host_info, const char* const* peer_host_info, size_t peer_host_info_num) {
  pthread_t serverId;
  int ret = pthread_create(&serverId, NULL, runServer, (void *)host_info);
  for (int i = 0; i < peer_host_info_num; i++) {
    global_peer_host_info[i] = std::string(peer_host_info[i]);
    std::string s = std::string(peer_host_info[i]);
    std::string ip, port;
    int flag = s.find(":");
    ip = s.substr(0,flag);
    port = s.substr(flag + 1, s.length());

    spdlog::info("Connect Server {}, ip: {}, port: {}", i, ip, port);
    for (int tid = 0; tid < 50; tid++) { // 50 tid
      while (true) {
        if (create_connect(ip.c_str(), stoi(port), tid, i) < 0) {
          std::this_thread::sleep_for(std::chrono::seconds(1));
        } else {
          break;
        }
      }
    }
    while (true) { // sync tid
      if (create_connect(ip.c_str(), stoi(port), SYNC_TID, i) < 0) {
        std::this_thread::sleep_for(std::chrono::seconds(1));
      } else {
        break;
      }
    }
    client_is_running[i].store(true);
  }

  group_is_runing.store(true);
  
  for (int i = 0; i < peer_host_info_num; i++) {
    while (true) {
      if (client_sync(4, i, SYNC_TID) > 0) {
        spdlog::info("Server {} init Success", i);
        break;
      } else {
        spdlog::info("Server {} init time out", i);
      }
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }
  std::this_thread::sleep_for(std::chrono::seconds(3));
}

static Package clientRemoteGet(int32_t select_column,
          int32_t where_column, const void *column_key, size_t column_key_len, int tid) {
  Package result;
  result.size = 0;
  bool is_find = false; //PK UK 找到就不找了
  for (int i = 0; i < 3; i++) {
    while (true) { // backoff
      if (!client_is_running[i].load() || is_find) break;
      // 杨樊：我们这边有个重要原则是：读取不会涉及已kill节点
      spdlog::debug("Begin Remote Get Select {}, where: {}, from {}", select_column, where_column, i);
      if (where_column == 0) pk_remote_count++;
      if (where_column == 1) uk_remote_count++;
      if (where_column == 3) sk_remote_count++;
      Package package = client_send(select_column, where_column, column_key, column_key_len, tid, i);
      if (package.size == -1) {
        client_is_running[i].store(false);
        break;
      }
      if (where_column == 0 || where_column == 1 && package.size > 0) is_find = true;
      int local_data_len, remote_data_len;
      if (select_column == 0 || select_column == 3) {
        local_data_len = result.size * 8;
        remote_data_len = package.size * 8;
      }
      else {
        local_data_len = result.size * 128;
        remote_data_len = package.size * 128;
      }
      if (local_data_len + remote_data_len > 2000 * 8) {
        spdlog::error("[clientRemoteGet] local_data_len + remote_data_len = {}, succeed 2000 * 8", local_data_len + remote_data_len);
      }
      memcpy(result.data + local_data_len, package.data, remote_data_len);
      result.size += package.size;
      break;
    }
  }
  return result;
}

static void deInitGroup() {
  group_is_deinit.store(true);
  for (int i = 0; i < PeerHostInfoNum; i++) {
    while (true) {
      if (!client_is_running[i].load()) break;
      int ret = client_sync(5, i, SYNC_TID);
      if (ret > 0) {
        spdlog::info("Server {} ready to deinit", i);
        break;
      } else if (ret == 0) {
        spdlog::info("Server {} not ready to deinit", i);
      } else {
        spdlog::info("Server {} is kill", i);
        break;
      }
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }
  std::this_thread::sleep_for(std::chrono::seconds(5));

  spdlog::info("Server remote get pk {}", pk_remote_count);
  spdlog::info("Server remote get uk {}", uk_remote_count);
  spdlog::info("Server remote get sk {}", sk_remote_count);
}