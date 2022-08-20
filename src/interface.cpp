#include "../inc/interface.h"
#include <iostream>

#define DEBUG_DB
#ifndef DEBUG_DB
#include "../inc/NvmDB.h"
NvmDB *db;
#else
#include "../inc/NvmDBC.h"
#endif

void engine_write(void *ctx, const void *data, size_t len) {
    #ifndef DEBUG_DB
    db->Put((char *)data, len);
    #else
    Put((char *)data, len);
    #endif 
}

size_t engine_read( void *ctx, int32_t select_column,
    int32_t where_column, const void *column_key, size_t column_key_len, void *res) {
    #ifndef DEBUG_DB
    return db->Get(select_column, where_column, column_key, column_key_len, res);
    #else
    return Get(select_column, where_column, column_key, column_key_len, res);
    #endif        
}

void* engine_init(const char* host_info, const char* const* peer_host_info, size_t peer_host_info_num,
                  const char* aep_dir, const char* disk_dir) {
    std::cout << "get sys pmem dir:" << aep_dir << std::endl;
    #ifndef DEBUG_DB
    db = new NvmDB(host_info, peer_host_info, peer_host_info_num, aep_dir, disk_dir);
    #else
    initNvmDB(host_info, peer_host_info, peer_host_info_num, aep_dir, disk_dir);
    #endif   
    std::cout << "recover succedd into check" << std::endl;

    return nullptr;
}

void engine_deinit(void *ctx) {
    #ifndef DEBUG_DB
    delete db;
    #else
    deinitNvmDB();
    #endif   
}
