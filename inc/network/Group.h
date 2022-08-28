#pragma once
#include <cstdio>
#include <string>
#include "./RestRPC/rest_rpc.hpp"
using namespace rest_rpc;
using namespace rest_rpc::rpc_service;

rpc_client clients[3];

struct Package {
  uint32_t size;
  std::string data;
  MSGPACK_DEFINE(size, data);
};

static Package remoteGet(rpc_conn conn, int32_t select_column,
          int32_t where_column, const std::string &column_key, size_t column_key_len);

static void initGroup(const char* host_info, const char* const* peer_host_info, size_t peer_host_info_num) {

  std::string s = std::string(host_info);
  int index = s.find(":");
  std::string port = s.substr(index + 1, s.length());
  rpc_server server(stoi(port), std::thread::hardware_concurrency());
  server.register_handler("remoteGet", remoteGet);
  server.run();

  for (int i = 0; i < peer_host_info_num; i++) {
    std::string s = std::string(peer_host_info[i]);
    std::string ip, port;
    int flag = s.find(":");
    ip = s.substr(0,flag);
    port = s.substr(flag + 1, s.length());
    std::cout << "Server " << i << " ip:" << ip << " port:" << port << std::endl;
    bool r = clients[i].connect(ip, stoi(port));
    while(!r) {
      r = clients[i].connect(ip, stoi(port));
    }
    std::cout << "Success Connect Server " << i << " ip:" << ip << " port:" << port << std::endl;
  }

}

static Package clientRemoteGet(int32_t select_column,
          int32_t where_column, const void *column_key, size_t column_key_len) {
  Package result;
  for (int i = 0; i < 3; i++) {
    try {
      Package package = clients[i].call<Package>("remoteGet", select_column, where_column, std::string((char *)column_key, column_key_len), column_key_len);
      result.size += package.size;
      result.data += package.data;
    } catch (const std::exception &e) {
      std::cout << e.what() << std::endl;
    }
  }
  return result;
}