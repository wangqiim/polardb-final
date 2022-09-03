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
  try {
    if (send(clients[server][tid], send_buf, buf_len, 0) < 0) {
      spdlog::error("Socket Send Server {} Failure, tid: {}", server, tid);
    } else {
      spdlog::debug("Socket Send Server {} Success, tid: {}", server, tid);
    }
  } catch (std::exception& e) {  
      spdlog::error("Socket Send Server {} tid: {} CoreDump {}, ", server, tid, e.what());
      page.size = -1;
      return page;
  }  


  int len = read(clients[server][tid], &page, sizeof(page));

  if (len == 0) {
    page.size = -1;
    return page;
  }
  return page;
}

int client_sync(uint8_t sync_type, int server, int tid) {
  bool result;
  try {
    if (send(clients[server][tid], &sync_type, 1, 0) < 0) {
      spdlog::error("Socket Send Server {} Failure, tid: {}", server, tid);
    } else {
      spdlog::debug("Socket Send Server {} Success, tid: {}", server, tid);
    }
  } catch (std::exception& e) {  
      spdlog::error("Socket Send Server {} tid: {} CoreDump {}, ", server, tid, e.what());
      return -1;
  }    

  int len = read(clients[server][tid], &result, sizeof(result));

  if (len == 0) {
    return -1;
  }

  if (result) {
    return 1;
  } 
  return 0;
}