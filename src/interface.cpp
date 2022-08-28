#include "../inc/interface.h"
#include <iostream>

#include "../inc/NvmDB.h"


void engine_write(void *ctx, const void *data, size_t len) {
    Put((char *)data, len);
}

size_t engine_read( void *ctx, int32_t select_column,
    int32_t where_column, const void *column_key, size_t column_key_len, void *res) {
    return Get(select_column, where_column, column_key, column_key_len, res, true);  
}

void* engine_init(const char* host_info, const char* const* peer_host_info, size_t peer_host_info_num,
                  const char* aep_dir, const char* disk_dir) {
    initNvmDB(host_info, peer_host_info, peer_host_info_num, aep_dir, disk_dir);

    return nullptr;
}

void engine_deinit(void *ctx) {
    deinitNvmDB();
}
