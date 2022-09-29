#pragma once
#include "spdlog/spdlog.h"

#include "MySocket.h"


typedef int CLIENTS_ARRAYS[3][MAX_Client_Num];

static CLIENTS_ARRAYS read_clients; // read时发起远程读的客户端, 初始化时，初始化前50 tid对应的客户端,剩余的当读线程来的时候，初始化
static CLIENTS_ARRAYS write_clients; // write时同步给其他节点的客户端, 初始化时，初始化前50 tid对应的客户端,剩余的写线程来的时候，初始化
static CLIENTS_ARRAYS sync_clients; // 发起同步的客户端，比如init,deinit. 初始化3个客户端用来同步init/deinit

int init_client_socket(CLIENTS_ARRAYS clients, const char *ip, int port, int tid, int server, RequestType request_type) {
  if ( (clients[server][tid] = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
    spdlog::error("Socket Create Failure, ip: {}, port: {}, tid: {}", ip, port, tid);
    return -1;
  }

  if (request_type != RequestType::SEND_SALARY) {
    int on = 1;
    if (setsockopt(clients[server][tid], IPPROTO_TCP, TCP_NODELAY, (char *)&on, sizeof(int)) < 0) {
        spdlog::error("set socket Close Nagle  error");
    }
  }
  setsockopt(clients[server][tid],SOL_SOCKET,SO_SNDBUF,(const char*)&NSendBuf,sizeof(int));
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
    // 对于用来发送salary的连接。提前发送一个requestType，告诉服务器连接类型。之后就不需要发request_type了。
    if (request_type == RequestType::SEND_SALARY) {
      ssize_t send_bytes = send(write_clients[server][tid], &request_type, sizeof(RequestType), 0);
      if (send_bytes != sizeof(RequestType)) {
        spdlog::error("[init_client_socket] send request_type through write clients fail!, errno = {}", errno);
      }
    }
    return 0;
  }
  return 0;
}

// return value
// 0: success
// 1: fail or error
int client_broadcast_send(uint8_t select_column,
          uint8_t where_column, const void *column_key, size_t column_key_len, int tid, int server) {
  if (read_clients[server][tid] == -1) {
    return -1;
  }
  char send_buf[20];
  size_t buf_len = 2 + column_key_len;
  memcpy(send_buf, &select_column, 1);
  memcpy(send_buf + 1, &where_column, 1);
  memcpy(send_buf + 2, column_key, column_key_len);
  ssize_t send_bytes = send(read_clients[server][tid], send_buf, buf_len, 0); 
  if (send_bytes <= 0) {
    if (send_bytes == 0) { // 远端关闭 eof
      spdlog::debug("[client_send] read eof!");
    } else {
      spdlog::warn("[client_send] Socket Send Server {} Failure, tid: {}, errno = {}", server, tid, errno);
    }
    read_clients[server][tid] = -1;
    return -1;
  } else {
    if (send_bytes != ssize_t(buf_len)) {
      spdlog::error("[client_send] send fail, send_bytes = {}, expected len: {}", send_bytes, buf_len);
      exit(1);
      read_clients[server][tid] = -1;
      return -1;
    }
    spdlog::debug("[client_send] Socket Send Server {} Success, tid: {}", server, tid);
  }
  return 0;
}

// return value
// Package.size = -1: fail or error
// else success
Package client_broadcast_recv(uint8_t select_column, int tid, int server) {
  Package page;
  if (read_clients[server][tid] == -1) {
    page.size = -1;
    return page;
  }
  // todo(wq): 直接read整个页应该也行.(不会有其他线程同时读写该socket)
  ssize_t len = read(read_clients[server][tid], &page, 4);
  if (len != 4) {
    spdlog::warn("[client_broadcast_recv] read fail, len = {}, expected: {}, errno = {}", len, 4, errno);
    page.size = -1;
    read_clients[server][tid] = -1;
    return page;
  }
  ssize_t value_len = 0;
  if (select_column == Id || select_column == Salary) {
    value_len = 8 * page.size;
  } else {
    value_len = 128 * page.size;
  }
  len = read(read_clients[server][tid], page.data, value_len);
  if (len != value_len) {
    if (len >= 0) {
      spdlog::warn("[client_broadcast_recv] read fail, len = {}, expected: {}, errno = {}", len, value_len, errno);
    } else {
      spdlog::error("[client_broadcast_recv] read fail, len = {}, expected: {}, errno = {}", len, value_len, errno);
    }
    page.size = -1;
    read_clients[server][tid] = -1;
  }
  return page;
}

// 把本地写的id和salary广播给其他节点
int client_salary_send(char *salary, int tid, int server) {
  if (write_clients[server][tid] == -1) {
    return -1;
  }
//  const int need_send_size = salary_page_cnt * 8;
  char send_buf[send_salary_page_size];
//  for (uint32_t i = 0; i < salary_page_cnt; ++i) {
//    memcpy(send_buf + i * 8, salary + i * MEM_RECORD_SIZE, 4); // 前4字节放id，后4字节放salary
//    memcpy(send_buf + i * 8 + 4, salary + i * MEM_RECORD_SIZE + 4, 4); // 前4字节放id，后4字节放salary
//  }
  memcpy(send_buf, salary, send_salary_page_size);
  ssize_t send_bytes = send(write_clients[server][tid], send_buf, send_salary_page_size, 0);
  if (send_bytes <= 0) {
    if (send_bytes == 0) { // 远端关闭 eof
      spdlog::debug("[client_salary_send] read eof!");
    } else {
      spdlog::warn("[client_salary_send] Socket Send Server {} Failure, tid: {}, errno = {}", server, tid, errno);
    }
    write_clients[server][tid] = -1;
    return -1;
  } else {
    if (send_bytes != send_salary_page_size) {
      spdlog::error("[client_salary_send] send fail, send_bytes = {}, expected len: {}", send_bytes, send_salary_page_size);
      exit(1);
      write_clients[server][tid] = -1;
      return -1;
    }
    spdlog::debug("[client_salary_send] Socket Send Server {} Success, tid: {}", server, tid);
  }
  return 0;
}

// 确认广播的ack
// 返回值: 0:成功，其他值:失败
int client_salary_recv(int tid, int server) {
  if (write_clients[server][tid] == -1) {
    return -1;
  }
  uint8_t response_code = 0;
  ssize_t len = read(write_clients[server][tid], &response_code, sizeof(uint8_t));
  if (len != 1) {
    spdlog::warn("[client_salary_recv] read fail, len = {}, expected: {}, errno = {}", len, sizeof(uint8_t), errno);
    write_clients[server][tid] = -1;
    return -1;
  }
  return 0;
}

// return_value: 
// 0: false
// 1: true
// -1: error
int client_sync(uint8_t sync_type, int server, int sync_tid) {
  bool result;
  int ret = 0;
  if ((ret = send(sync_clients[server][sync_tid], &sync_type, 1, 0)) <= 0) {
    spdlog::warn("[client_sync] Socket Send Server {} Failure, sync_tid: {}, ret = {}, errno = {}", server, sync_tid, ret, errno);
    return -1;
  } else {
    spdlog::debug("[client_sync] Socket Send Server {} Success, sync_tid: {}", server, sync_tid);
  }

  int len = read(sync_clients[server][sync_tid], &result, sizeof(result));

  if (len <= 0) {
    return -1;
  }

  if (result) {
    return 1;
  } 
  return 0;
}
