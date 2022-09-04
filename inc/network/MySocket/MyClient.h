#pragma mark once
#include <exception>
#include "./MyServer.h"

static int clients[3][50];

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

  struct timeval timeout = {3,0};
  if (setsockopt(clients[server][tid], SOL_SOCKET,SO_SNDTIMEO, (char *)&timeout,sizeof(struct timeval)) < 0) {
    spdlog::error("set client socket send time out error");
  }
  if (setsockopt(clients[server][tid], SOL_SOCKET, SO_RCVTIMEO, (char *)&timeout, sizeof(struct timeval)) < 0) {
    spdlog::error("set client socket recv time out error");
  }
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
    spdlog::error("Socket Send Server {} Failure or Time out, tid: {}", server, tid);
    page.size = -1;
    return page;
  } else {
    if (send_bytes != buf_len) {
      spdlog::error("[client_send] send fail, send_bytes = {}, expected len: {}", send_bytes, buf_len);
      exit(1);
    }
    spdlog::debug("Socket Send Server {} Success, tid: {}", server, tid);
  }

  int len = read(clients[server][tid], &page, sizeof(page));
  if (len <= 0) {
    spdlog::error("[client_send] read fail or time out, len = {}, expected: {}", len, sizeof(page));
    page.size = -1;
    return page;
  }
  return page;
}

int client_sync(uint8_t sync_type, int server, int tid) {
  bool result;
  if (send(clients[server][tid], &sync_type, 1, 0) <= 0) {
    spdlog::error("Socket Send Server {} Failure, tid: {}", server, tid);
    return -1;
  } else {
    spdlog::debug("Socket Send Server {} Success, tid: {}", server, tid);
  }

  int len = read(clients[server][tid], &result, sizeof(result));

  if (len <= 0) {
    return -1;
  }

  if (result) {
    return 1;
  } 
  return 0;
}