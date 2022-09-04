#pragma mark once
#include <exception>
#include "./MyServer.h"

static int clients[3][51]; // clients[i][50] 仅仅用来同步

int create_connect(const char *ip, int port, int tid, int server) {
  if ( (clients[server][tid] = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    spdlog::error("Socket Create Failure, ip: {}, port: {}, tid: {}", ip, port, tid);
    return -1;
  } 
  struct sockaddr_in serv_addr;//首先要指定一个服务端的ip地址+端口，表明是向哪个服务端发起请求
  memset(&serv_addr, 0, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_addr.s_addr = inet_addr(ip);//注意，这里是服务端的ip和端口
  serv_addr.sin_port = htons(port);

  if (connect(clients[server][tid], (struct sockaddr*)&serv_addr, sizeof(serv_addr)) < 0) {
    spdlog::error("Socket Connect Failure, ip: {}, port: {}, tid: {}", ip, port, tid);
    return -1;
  } else {
    spdlog::debug("Socket Connect Success, ip: {}, port: {}, tid: {}", ip, port, tid);
    return 0;
  }
  return 0;
}

Package client_send(uint8_t select_column,
          uint8_t where_column, const void *column_key, size_t column_key_len, int tid, int server) {
  char send_buf[130];
  Package page;
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
    page.size = -1;
    return page;
  } else {
    if (send_bytes != buf_len) {
      spdlog::error("[client_send] send fail, send_bytes = {}, expected len: {}", send_bytes, buf_len);
      exit(1);
    }
    spdlog::debug("[client_send] Socket Send Server {} Success, tid: {}", server, tid);
  }

  int len = read(clients[server][tid], &page, 4);
  if (len != 4) {
    spdlog::warn("[client_send] read fail, len = {}, expected: {}", len, 4);
    page.size = -1;
    return page;
  }
  size_t value_len;
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
    return page;
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
