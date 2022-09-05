#pragma mark once
#include <sys/types.h> /* See NOTES */
#include <sys/socket.h>
#include <netinet/tcp.h> 
#include <arpa/inet.h>
#include <string.h>
#include <stdio.h>
#include <unistd.h>
// #include "Config.h"
#include "spdlog/spdlog.h"

#include <pthread.h>
#include <stdio.h>
#include <errno.h>
#include <ctype.h>
#include <exception> 
#include <sstream>
#include <iomanip>

std::string to_hex(unsigned char* data, int len) {
    std::stringstream ss;
    ss << std::uppercase << std::hex << std::setfill('0');
    for (int i = 0; i < len; i++) {
        ss << std::setw(2) << static_cast<unsigned>(data[i]);
    }
    return ss.str();
}

struct s_info {
    sockaddr_in s_addr;
    int fd;
} ts[256];

struct Package {
  uint32_t size = 0;
  char data[2000 * 8];
};

// Select 1 Byte Where 1 Byte CloumKey max 128 Bytes
const int BUFSIZE = 130;

static Package remoteGet(int32_t select_column,
          int32_t where_column, char *column_key, size_t column_key_len);

static bool serverSyncInit();
static bool serverSyncDeinit();

void *connect_client(void *arg) {
    struct s_info *ts = (struct s_info *)arg;
    ssize_t size_len = 0;
    char buf[BUFSIZE];
    while (1) {
        size_len = read(ts->fd, buf, BUFSIZE);
        if (size_len <= 0) {
            if (size_len == 0) {
                spdlog::debug("[connect_client] close");
            } else {
                spdlog::error("[connect_client] read error");
            }
            close(ts->fd);
            pthread_exit(NULL);
        }
        uint8_t selectColum = *(uint8_t *)buf;
        //判断同步启动 || 关闭
        if (selectColum == 4 || selectColum == 5) {
            bool result;
            if (selectColum == 4) {
                result = serverSyncInit();
            } else {
                result = serverSyncDeinit();
            }
            int writen_bytes = write(ts->fd, &result, sizeof(result));
            if (writen_bytes < 0) {
                spdlog::error("[connect_client] server_socket sync error, errno = {}", errno);
            } else if (writen_bytes != 1) {
                spdlog::error("[connect_client] sync write fail, writen_bytes = {}, expected = {}", writen_bytes, 1);
            }
            continue;            
        }
        uint8_t whereColum = *(uint8_t *)(buf + 1);
        char *column_key = buf + 2;

        size_t column_key_len = 8;
        spdlog::debug("[connect_client] select = {}, where = {}, key = {}",selectColum, whereColum, *(uint64_t *)column_key);

        if (size_len != 2 + column_key_len) {
            spdlog::error("[connect_client] read error, size_len = {}, expected: {}",size_len , 2 + column_key_len);
        }
        Package page = remoteGet(selectColum, whereColum, column_key, column_key_len);

        if (selectColum == 0 || selectColum == 3) {
            column_key_len = 8;
        } else {
            column_key_len = 128;
        }
        size_t data_size = page.size * column_key_len + 4; //数据长度加上Size
        int writen_bytes = write(ts->fd, &page, data_size);
        if (writen_bytes != data_size) {
            spdlog::error("[connect_client] server_socket write error, writen_bytes = {}, expected: {}", writen_bytes, data_size);
        }
    }
}

static void my_server_run(const char *ip, int port) {
    int server_socket = socket(AF_INET, SOCK_STREAM, 0);
    if (server_socket < 0) {
        spdlog::error("server_socket create error");
    } else {
         spdlog::debug("server_socket create success");
    }
    int opt = 1;
    if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, (void *)&opt, sizeof(opt)) < 0) {
        spdlog::error("set socket error");
    } else {
        spdlog::debug("set socket success");
    }
    int on = 1;
    if (setsockopt(server_socket, IPPROTO_TCP, TCP_NODELAY, (char *)&on, sizeof(int)) < 0) {
        spdlog::error("set socket Close Nagle  error");
    }

    sockaddr_in addr, client;
    socklen_t len = sizeof(client);

    addr.sin_addr.s_addr =  htonl(INADDR_ANY);
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    if (bind(server_socket, (struct sockaddr*)&addr, sizeof(addr)) < 0) {
        spdlog::error("bind socket error, ip {}", ip);
    } else {
        spdlog::debug("bind socket success");
    }

    if (listen(server_socket, 127) < 0) { // todo(wq): 需要大于150吗？
        spdlog::error("listen socket error, ip {}", ip);
    } else {
        spdlog::debug("listen socket success");
    }

    int i = 0;
    while (1) {
        int client_fd = accept(server_socket, (struct sockaddr*)&client, &len);
        if (client_fd < 0) {
            spdlog::error("accept socket error, ip {}", ip);
        } else {
            spdlog::debug("accept socket success, ip {}", ip);
        }
        ts[i].s_addr = client;
        ts[i].fd = client_fd;

        pthread_t tid;
        pthread_create(&tid, NULL, connect_client, (void *)&ts[i]);
        pthread_detach(tid);
        i++;
    }
}

