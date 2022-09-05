#pragma mark once
#include <exception>
#include "./MyServer.h"

// 初始化时，初始化前50 tid对应的客户端，以及最后一个客户端（用来同步init/deinit）,剩余的当读线程来的时候，初始化
static int clients[3][SYNC_TID + 1]; // clients[i][SYNC_TID] 仅仅用来同步

int create_connect(const char *ip, int port, int tid, int server) {
  if ( (clients[server][tid] = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    spdlog::error("Socket Create Failure, ip: {}, port: {}, tid: {}", ip, port, tid);
    return -1;
  }

  int on = 1;
  if (setsockopt(clients[server][tid], IPPROTO_TCP, TCP_NODELAY, (char *)&on, sizeof(int)) < 0) {
      spdlog::error("set socket Close Nagle  error");
  } 
  struct sockaddr_in serv_addr;//首先要指定一个服务端的ip地址+端口，表明是向哪个服务端发起请求
  memset(&serv_addr, 0, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_addr.s_addr = inet_addr(ip);//注意，这里是服务端的ip和端口
  serv_addr.sin_port = htons(port);

  if (connect(clients[server][tid], (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
    spdlog::warn("[create_connect] Socket Connect Failure, ip: {}, port: {}, tid: {}", ip, port, tid);
    return -1;
  } else {
    spdlog::debug("[create_connect] Socket Connect Success, ip: {}, port: {}, tid: {}", ip, port, tid);
    return 0;
  }
  return 0;
}

// return value
// 0: success
// 1: fail or error
int client_broadcast_send(uint8_t select_column,
          uint8_t where_column, const void *column_key, size_t column_key_len, int tid, int server) {
  int ret = 0;
  char send_buf[20];
  size_t buf_len = 2 + column_key_len;
  memcpy(send_buf, &select_column, 1);
  memcpy(send_buf + 1, &where_column, 1);
  memcpy(send_buf + 2, column_key, column_key_len);
  int send_bytes = send(clients[server][tid], send_buf, buf_len, 0); 
  if (send_bytes <= 0) {
    if (send_bytes == 0) { // 远端关闭 eof
      spdlog::debug("[client_send] read eof!");
    } else {
      spdlog::warn("[client_send] Socket Send Server {} Failure, tid: {}, errno = {}", server, tid, errno);
    }
    return -1;
  } else {
    if (send_bytes != buf_len) {
      spdlog::error("[client_send] send fail, send_bytes = {}, expected len: {}", send_bytes, buf_len);
      exit(1);
      return -1;
    }
    spdlog::debug("[client_send] Socket Send Server {} Success, tid: {}", server, tid);
  }
  return 0;
}

// return value
// Package.size = -1: fail or error
// else success
Package client_broadcast_recv(uint8_t select_column,
          uint8_t where_column, const void *column_key, size_t column_key_len, int tid, int server) {
  Package page;
  // todo(wq): 直接read整个页应该也行.(不会有其他线程同时读写该socket)
  int len = read(clients[server][tid], &page, 4);
  if (len != 4) {
    spdlog::warn("[client_send] read fail, len = {}, expected: {}", len, 4);
    page.size = -1;
    return page;
  }
  size_t value_len = 0;
  if (select_column == 0 || select_column == 3) {
    value_len = 8 * page.size;
  } else {
    value_len = 128 * page.size;
  }
  len = read(clients[server][tid], page.data, value_len);
  if (len != value_len) {
    if (len >= 0) {
      spdlog::warn("[client_send] read fail, len = {}, expected: {}", len, value_len);
    } else {
      spdlog::error("[client_send] read fail, len = {}, expected: {}, errno = {}", len, value_len, errno);
    }
    page.size = -1;
  }
  return page;
}

// return_value: 
// 0: false
// 1: true
// -1: error
int client_sync(uint8_t sync_type, int server, int sync_tid) {
  bool result;
  if (send(clients[server][sync_tid], &sync_type, 1, 0) <= 0) {
    spdlog::warn("[client_sync] Socket Send Server {} Failure, sync_tid: {}", server, sync_tid);
    return -1;
  } else {
    spdlog::debug("[client_sync] Socket Send Server {} Success, sync_tid: {}", server, sync_tid);
  }

  int len = read(clients[server][sync_tid], &result, sizeof(result));

  if (len <= 0) {
    return -1;
  }

  if (result) {
    return 1;
  } 
  return 0;
}
