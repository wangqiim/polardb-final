#pragma once
#include "spdlog/spdlog.h"

#include "MySocket.h"
#include <omp.h>

uint64_t salary_sync_cnt = 0;
std::mutex salary_sync_cnt_mtx;
std::condition_variable salary_sync_cnt_cv;
const uint32_t Salary_Cache_Num = 6e8;

std::string to_hex(unsigned char* data, int len) {
    std::stringstream ss;
    ss << std::uppercase << std::hex << std::setfill('0');
    for (int i = 0; i < len; i++) {
        ss << std::setw(2) << static_cast<unsigned>(data[i]);
    }
    return ss.str();
}

// Select 1 Byte Where 1 Byte CloumKey max 128 Bytes
const int BUFSIZE = 130;
const int Salary_BUFSIZE = 8 * int(2e6); // note(wq): 开大一点，在for循环中replay其他节点索引的时候，也许可以降低更多的同步开销？
char Salary_Buf[3][Salary_BUFSIZE];
const int OMP_THREAD_NUM = 32;


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
                spdlog::error("[connect_client] read error, errno = {}", errno);
            }
            close(ts->fd);
            pthread_exit(NULL);
        }
        RequestType request_type = *(RequestType *)buf;
        //判断同步启动 || 关闭
        if (request_type == RequestType::SYNC_INIT || request_type == RequestType::SYNC_DEINIT) {
            bool result;
            if (request_type == RequestType::SYNC_INIT) {
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
        } else if (request_type == RequestType::SEND_SALARY) {
            omp_set_num_threads(OMP_THREAD_NUM);
            // 1. 接受一个 requestType
            if (size_len != sizeof(RequestType)) {// 接收一个requestType，确认连接类型。之后就不需要发requesttype了。
                spdlog::error("[connect_client] read SEND_SALARY request_type, size_len = {}, errno = {}", size_len, errno);
            }
            char * const salary_buf = Salary_Buf[ts->peer_idx];
            // 2. 接受一个 offset
            uint64_t offset;
            size_len = read(ts->fd, &offset, sizeof(offset));
            if (size_len != sizeof(offset)) {
                spdlog::error("[connect_client] read SEND_SALARY request_type, size_len = {}, errno = {}", size_len, errno);
            }
            peer_offset[ts->peer_idx] = offset;
            // 3. 循环读socket，构建id,salary索引
            char *salary_ptr = salary_buf;
            ssize_t unprocessed_len = 0; //读了以后，还未处理的长度
            uint64_t id = offset;
            while (true) {
                if (salary_ptr - salary_buf == Salary_BUFSIZE) {
                    salary_ptr = salary_buf;
                }
                size_len = read(ts->fd, salary_ptr + unprocessed_len, (salary_buf + Salary_BUFSIZE) - (salary_ptr + unprocessed_len)); // 尝试读满buf
                unprocessed_len += size_len;
                if (size_len <= 0) {
                    spdlog::error("[connect_client] read SEND_SALARY fail, size_len = {}, errno = {}", size_len, errno);
                    close(ts->fd);
                    pthread_exit(NULL);
                }
                ssize_t unprocessed_num = unprocessed_len / 8;
                uint64_t tmp_id = id;
#pragma omp parallel for
                for (ssize_t i = 0; i < unprocessed_num; i++) {
                  insertRemoteIdToSK(ts->peer_idx, (uint32_t)(tmp_id + i), *(uint32_t *)(salary_ptr + 8 * i));
                  insertRemoteSalaryToPK(tmp_id + i, *(uint64_t *)(salary_ptr + 8 * i));
                }
                salary_ptr += unprocessed_num * 8;
                unprocessed_len -= unprocessed_num * 8;
                id += unprocessed_num;
                
                if (id == offset + TOTAL_WRITE_NUM) {
                    std::unique_lock guard(salary_sync_cnt_mtx);
                    salary_sync_cnt += TOTAL_WRITE_NUM;
                    if (salary_sync_cnt == Salary_Cache_Num) {
                        salary_sync_cnt_cv.notify_all();
                    }
                }
            }
        }
        uint8_t selectColum = (uint8_t)request_type;
        uint8_t whereColum = *(uint8_t *)(buf + 1);
        char *column_key = buf + 2;

        ssize_t column_key_len = 8;
        spdlog::debug("[connect_client] select = {}, where = {}, key = {}",selectColum, whereColum, *(uint64_t *)column_key);

        if (size_len != 2 + column_key_len) {
            spdlog::error("[connect_client] read error, size_len = {}, expected: {}",size_len , 2 + column_key_len);
        }
        Package page = remoteGet(selectColum, whereColum, column_key, column_key_len);

        if (selectColum == Id || selectColum == Salary) {
            column_key_len = 8;
        } else {
            column_key_len = 128;
        }
        ssize_t data_size = page.size * column_key_len + 4; //数据长度加上Size
        ssize_t writen_bytes = write(ts->fd, &page, data_size);
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
        if (i >= MAX_LISTEN_CONN) {
            spdlog::error("listen fd exceed max listen num {}", MAX_LISTEN_CONN);
        }
        int client_fd = accept(server_socket, (struct sockaddr*)&client, &len);
        if (client_fd < 0) {
            spdlog::error("accept socket error, ip {}", ip);
        } else {
            spdlog::debug("accept socket success, ip {}", ip);
        }
        ts[i].s_addr = client;
        ts[i].fd = client_fd;
        ts[i].peer_idx = -1;
        for (size_t idx = 0; idx < PeerHostInfoNum; idx++) {
            std::string s = global_peer_host_info[idx];
            std::string ip, port;
            int flag = s.find(":");
            ip = s.substr(0,flag);
            if (ts[i].s_addr.sin_addr.s_addr == inet_addr(ip.c_str())) {
                ts[i].peer_idx = idx;
                break;
            }
        }
        if (ts[i].peer_idx == -1) {
            spdlog::error("[my_server_run] set peer_idx fail!");
            exit(0);
        }

        pthread_t tid;
        pthread_create(&tid, NULL, connect_client, (void *)&ts[i]);
        pthread_detach(tid);
        i++;
    }
}

