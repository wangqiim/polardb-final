#pragma once

#include <sys/types.h> /* See NOTES */
#include <sys/socket.h>
#include <netinet/tcp.h> 
#include <arpa/inet.h>
#include <pthread.h>
#include <stdio.h>
#include <errno.h>
#include <ctype.h>
#include <exception> 
#include <sstream>
#include <iomanip>
#include "Config.h"
#include <string.h>
#include <unistd.h>

std::string global_local_host_info;
std::vector<std::string> global_peer_host_info;

enum class RequestType : uint8_t {
    WHERE_ID, // default 0
    WHERE_USERID,
    WHERE_NAME,
    WHERE_SALARY,
    SYNC_INIT,
    SYNC_DEINIT,
    SEND_SALARY,
    NONE
};

const int MAX_LISTEN_CONN = 512;
struct s_info {
    sockaddr_in s_addr;
    int fd;
    int peer_idx;
} ts[MAX_LISTEN_CONN];

const int PACKAGE_DATA_SIZE = 1000 * 8;

struct Package {
  int32_t size = 0;
  char data[PACKAGE_DATA_SIZE];
};

const int NSendBuf = 50000 * 8;
const int NRecvBuf = 50000 * 8;
