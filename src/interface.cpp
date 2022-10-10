#include "../inc/interface.h"
#include <iostream>
#include "spdlog/spdlog.h"
#include <signal.h>

#include "../inc/NvmDB.h"

int call_init_num = 0;

void engine_write(__attribute__((unused)) void *ctx, const void *data, size_t len) {
    Put((char *)data, len);
}

size_t engine_read(__attribute__((unused)) void *ctx, int32_t select_column,
    int32_t where_column, const void *column_key, size_t column_key_len, void *res) {
    return Get(select_column, where_column, column_key, column_key_len, res, true); 
}

void* engine_init(const char* host_info, const char* const* peer_host_info, size_t peer_host_info_num,
                  const char* aep_dir, const char* disk_dir) {
    spdlog::set_level(spdlog::level::info);
    signal(SIGPIPE, SIG_IGN);  // 忽略 SIGPIPE 信号
    call_init_num++;
    spdlog::info("version [Store sync], call engine_init times = {}", call_init_num);
    initNvmDB(host_info, peer_host_info, peer_host_info_num, aep_dir, disk_dir);

    return nullptr;
}

void engine_deinit(__attribute__((unused)) void *ctx) {
    deinitNvmDB();
}
