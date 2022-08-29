#pragma once
#include <cstdio>
#include <string>
#include "./RestRPC/rest_rpc.hpp"
using namespace rest_rpc;
using namespace rest_rpc::rpc_service;

rpc_client clients[3];
rpc_server *server;

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

void *runServer(void *input) {
  int port = *(int *)input;
  server = new rpc_server(port, std::thread::hardware_concurrency());
  server -> register_handler("remoteGet", remoteGet);
  server -> register_handler("serverSyncInit", serverSyncInit);
  std::cout << "Success Run port: " << port << std::endl;
  server -> run();
}


static void initGroup(const char* host_info, const char* const* peer_host_info, size_t peer_host_info_num) {
  std::string s = std::string(host_info);
  int index = s.find(":");
  std::string port = s.substr(index + 1, s.length());
  for (int i = 0; i < peer_host_info_num; i++) {
    std::cout << "peer host info " << i << " " << peer_host_info[i] << std::endl;
  }

  pthread_t serverId;
  int portInt = stoi(port);
  int ret = pthread_create(&serverId, NULL, runServer, &portInt);

  for (int i = 0; i < peer_host_info_num; i++) {
    std::string s = std::string(peer_host_info[i]);
    std::string ip, port;
    int flag = s.find(":");
    ip = s.substr(0,flag);
    port = s.substr(flag + 1, s.length());
    std::cout << "Server " << i << " ip:" << ip << " port:" << port << std::endl;
    clients[i].enable_auto_reconnect(); // automatic reconnect
    clients[i].enable_auto_heartbeat(); // automatic heartbeat
    bool r = clients[i].connect(ip, stoi(port));
    while (true) {
      if (clients[i].has_connected()) {
        std::cout << "Success Connect Server " << i << " ip:" << ip << " port:" << port << std::endl;
        break;
      } else {
       std::cout << "Failed Connect Server " << i << " ip:" << ip << " port:" << port << std::endl;
      }
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }

  for (int i = 0; i < peer_host_info_num; i++) {
    while (true) {
      if (clients[i].call<bool>("serverSyncInit")) {
        std::cout << "Server " << i << "init Success" << std::endl;
        break;
      } else {
        std::cout << "Server " << i << "init time out" << std::endl;
      }
      std::this_thread::sleep_for(std::chrono::seconds(1));
    }
  }

}

static Package clientRemoteGet(int32_t select_column,
          int32_t where_column, const void *column_key, size_t column_key_len) {
  Package result;
  for (int i = 0; i < 3; i++) {
    try {
      std::cout << "Get Select " << select_column << " where " << where_column << " from " << i << std::endl;
      std::string key = std::string((char *)column_key, column_key_len);
      Package package = clients[i].call<Package>("remoteGet", select_column, where_column, key, column_key_len);
      result.size += package.size;
      result.data += package.data;
    } catch (const std::exception &e) {
      std::cout << e.what() << std::endl;
    }
  }
  return result;
}

