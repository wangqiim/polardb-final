#pragma once
#include <cstdio>
#include <string>
#include "./RestRPC/rest_rpc.hpp"
#include "Config.h"
#include "spdlog/spdlog.h"

using namespace rest_rpc;
using namespace rest_rpc::rpc_service;

rpc_client clients[3];
rpc_server *server;

bool group_is_deinit;

struct Package {
  uint32_t size = 0;
  std::string data = "";
  MSGPACK_DEFINE(size, data);
};

static Package remoteGet(rpc_conn conn, int32_t select_column,
          int32_t where_column, const std::string &column_key, size_t column_key_len);

static bool serverSyncInit(rpc_conn conn) {
  for (int i = 0; i < 3; i++) {
    bool isInit = clients[i].has_connected();
    if (!isInit) return false; 
  }
  return true;
}

static bool serverSyncDeinit(rpc_conn conn) {
  return group_is_deinit;
}

void *runServer(void *input) {
  int port = *(int *)input;
  server = new rpc_server(port, std::thread::hardware_concurrency());
  server -> register_handler("remoteGet", remoteGet);
  server -> register_handler("serverSyncInit", serverSyncInit);
  server -> register_handler("serverSyncDeinit", serverSyncDeinit);
  spdlog::info("Local rpc_Server Success Run port: {}", port);
  server -> run();
}


static void initGroup(const char* host_info, const char* const* peer_host_info, size_t peer_host_info_num) {
  std::string s = std::string(host_info);
  int index = s.find(":");
  std::string port = s.substr(index + 1, s.length());
  for (int i = 0; i < peer_host_info_num; i++) {
    spdlog::info("peer host info {}, {}", i, peer_host_info[i]);
  }

  group_is_deinit = false;

  pthread_t serverId;
  int portInt = stoi(port);
  int ret = pthread_create(&serverId, NULL, runServer, &portInt);

  for (int i = 0; i < peer_host_info_num; i++) {
    std::string s = std::string(peer_host_info[i]);
    std::string ip, port;
    int flag = s.find(":");
    ip = s.substr(0,flag);
    port = s.substr(flag + 1, s.length());
    spdlog::info("Server {}, ip: {}, port: {}", i, ip, port);
    clients[i].enable_auto_reconnect(); // automatic reconnect
    clients[i].enable_auto_heartbeat(); // automatic heartbeat
    bool r = clients[i].connect(ip, stoi(port));
    while (true) {
      if (clients[i].has_connected()) {
        spdlog::info("Success Connect Server {}, ip: {}, port: {}", i, ip, port);
        break;
      } else {
        spdlog::info("Failed Connect Server  {}, ip: {}, port: {}", i, ip, port);
      }
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }

  for (int i = 0; i < peer_host_info_num; i++) {
    while (true) {
      if (clients[i].call<bool>("serverSyncInit")) {
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
          int32_t where_column, const void *column_key, size_t column_key_len) {
  Package result;
  for (int i = 0; i < 3; i++) {
    while (true) { // backoff
      try {
        // 杨樊：我们这边有个重要原则是：读取不会涉及已kill节点
        if (!clients[i].has_connected()) {
          break;
        }
        spdlog::debug("Get Select {}, where: {}, from {}", select_column, where_column, i);
        std::string key = std::string((char *)column_key, column_key_len);
        Package package = clients[i].call<Package>("remoteGet", select_column, where_column, key, column_key_len);
        result.size += package.size;
        result.data += package.data;
        break;
      } catch (const std::exception &e) {
        spdlog::error("Get Error {}", e.what());
        clients[i].close();
      }
    }
  }
  return result;
}

static void deInitGroup() {
  group_is_deinit = true;
  for (int i = 0; i < PeerHostInfoNum; i++) {
    while (true) {
      if (clients[i].call<bool>("serverSyncDeinit")) {
        spdlog::info("Server {} ready to deinit", i);
        break;
      } else {
        spdlog::info("Server {} not ready to deinit", i);
      }
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }
  std::this_thread::sleep_for(std::chrono::seconds(4));
}