#include "../inc/interface.h"
#include <iostream>

#include "../inc/NvmDB.h"


void engine_write(void *ctx, const void *data, size_t len) {
    Put((char *)data, len);
}

size_t engine_read( void *ctx, int32_t select_column,
    int32_t where_column, const void *column_key, size_t column_key_len, void *res) {
    return Get(select_column, where_column, column_key, column_key_len, res, true); 
    // rest_rpc::rpc_service::rpc_conn coon; 
    // Package packge =  remoteGet(coon, select_column, where_column, std::string((char *)column_key, column_key_len), column_key_len);
    // int dataSize = 0;
    // if(select_column == Id || select_column == Salary) dataSize = packge.size * 8;
    // if(select_column == Userid || select_column == Name) dataSize = packge.size * 128;
    // memcpy(res, packge.data.c_str(), dataSize);
    // return packge.size;
}

void* engine_init(const char* host_info, const char* const* peer_host_info, size_t peer_host_info_num,
                  const char* aep_dir, const char* disk_dir) {
    initNvmDB(host_info, peer_host_info, peer_host_info_num, aep_dir, disk_dir);

    return nullptr;
}

void engine_deinit(void *ctx) {
    deinitNvmDB();
}
