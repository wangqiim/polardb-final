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
  return nullptr;
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
  if (ret != 0) {
    spdlog::error("[initGroup] pthread_create error, ret = {}", ret);
  }
  // 给peer_host_info排序
  std::string host_info_str(host_info);
  std::vector<std::string> all_host_info_str;
  all_host_info_str.push_back(host_info_str);
  for (size_t i = 0; i < peer_host_info_num; i++) {
    all_host_info_str.push_back(std::string(peer_host_info[i]));
  }
  std::sort(all_host_info_str.begin(), all_host_info_str.end());
  size_t idx = 0;
  for (idx = 0; idx < all_host_info_str.size(); idx++) {
    if (all_host_info_str[idx] == host_info_str) break;
  }
  std::vector<std::string> sorted_peer_host_info;
  int t = peer_host_info_num; // 3
  while (t--) {
    idx = (idx + 1) % all_host_info_str.size();
    sorted_peer_host_info.push_back(all_host_info_str[idx]);
  }

  for (size_t i = 0; i < peer_host_info_num; i++) {
    global_peer_host_info[i] = sorted_peer_host_info[i];
    std::string s = sorted_peer_host_info[i];
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
  }

  group_is_runing.store(true);
  
  for (size_t i = 0; i < peer_host_info_num; i++) {
    while (true) {
      if (client_sync(4, i, SYNC_TID) > 0) {
        spdlog::info("Server {} init Success", i);
        break;
      } else {
        spdlog::info("Server {} init time out", i);
      }
      std::this_thread::sleep_for(std::chrono::milliseconds(10));
    }
  }
  std::this_thread::sleep_for(std::chrono::seconds(3));
}

// 1. 依次向3个节点发送请求(write)，发送完以后进入第二步
// 2. 依次从3个节点读取数据(read)
static Package clientRemoteGet(int32_t select_column,
          int32_t where_column, const void *column_key, size_t column_key_len, int tid) {
  //每个线程自己维护remote_server
  static thread_local uint8_t remote_pk_in_client[4] = {0,0,0,0};
  // 1. broadcast phrase1: send
  uint64_t id_to_server = (*(uint64_t *)column_key) / 200000000;
  bool pk_has_find_server = false;
  for (int i = 0; i < PeerHostInfoNum; i++) {
    if (where_column == 0 && is_use_remote_pk && id_to_server < 4 && remote_pk_in_client[id_to_server] > 0) {
      if(i != remote_pk_in_client[id_to_server] - 1) {
        continue;
      }
      pk_has_find_server = true;
    }
    // 不需要检验ret,如果发送出错，读的时候也会出错，不会永远阻塞住。
    client_broadcast_send(select_column, where_column, column_key, column_key_len, tid, i);
  }
  // 2. broadcast phrase2: recv
  Package result;
  result.size = 0;
  for (int i = 0; i < PeerHostInfoNum; i++) {
    if (pk_has_find_server && i != remote_pk_in_client[id_to_server] - 1) continue;
    Package package = client_broadcast_recv(select_column, tid, i);
    if (package.size == -1) {
      continue;
    }

    //如果查PK，并且查到了
    if (where_column == 0 && package.size > 0) {
      if (id_to_server < 4) {
        //标记要去哪找
        remote_pk_in_client[id_to_server] = i + 1;
      }
    }

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
  }
  return result;
}

static void deInitGroup() {
  group_is_deinit.store(true);
  for (int i = 0; i < PeerHostInfoNum; i++) {
    while (true) {
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
}