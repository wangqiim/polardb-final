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

enum class RequestType : uint8_t {
    WHERE_ID, // default 0
    WHERE_USERID,
    WHERE_NAME,
    WHERE_SALARY,
    SYNC_INIT,
    SYNC_DEINIT
};

struct s_info {
    sockaddr_in s_addr;
    int fd;
} ts[256];

const int PACKAGE_DATA_SIZE = 2000 * 8;

struct Package {
  int32_t size = 0;
  char data[PACKAGE_DATA_SIZE];
};
