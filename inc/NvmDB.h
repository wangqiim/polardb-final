#pragma once
#include <cstdint>
#include <cstdio>
#include "./network/Group.h"
#include "./store/NvmStore.h"

static void initNvmDB(const char* host_info, const char* const* peer_host_info, size_t peer_host_info_num,
                const char* aep_dir, const char* disk_dir){
    initStore(aep_dir, disk_dir);
    initGroup(host_info, peer_host_info, peer_host_info_num);
}

static std::atomic<uint8_t> putTid(0);
static void Put(const char *tuple, size_t len){
    static thread_local uint8_t tid = putTid++;
    static thread_local int write_count = 0;
    writeTuple(tuple, len, tid);
    insert(tuple, len, tid);
    write_count++;
    if (write_count % 1000000 == 0) {
      std::cout << "thread " << tid <<  " write " << write_count << std::endl;
    }
}

static size_t Get(int32_t select_column,
          int32_t where_column, const void *column_key, size_t column_key_len, void *res, bool is_local){
    std::vector<uint32_t> posArray = getPosFromKey(where_column, column_key);
    if (posArray.size() > 0){
      for (uint32_t pos : posArray) {
        readColumFromPos(select_column, pos, res);
        if(select_column == Id || select_column == Salary) res += 8;
        if(select_column == Userid || select_column == Name) res += 128;
      }
      if (where_column != Salary) return posArray.size();
    }
    if ((posArray.size() == 0 || where_column == Salary) && is_local) {
      Package result = clientRemoteGet(select_column, where_column, column_key, column_key_len);
      int dataSize = 0;
      if(select_column == Id || select_column == Salary) dataSize = result.size * 8;
      if(select_column == Userid || select_column == Name) dataSize = result.size * 128;      
      memcpy(res, result.data.c_str(), dataSize);
      return result.size + posArray.size(); 
    }
    return 0;
}

static Package remoteGet(rpc_conn conn, int32_t select_column,
          int32_t where_column, const std::string &column_key, size_t column_key_len) {
  char *res = new char[2000 * 8];
  Package packge;
  if (where_column == Salary || where_column == Id) {
    uint64_t key = *(uint64_t *)(column_key.c_str());
    std::cout << "Remote Get Select " << select_column << " where " << where_column << " key " << key << std::endl;
  } else {
    std::cout << "Remote Get Select " << select_column << " where " << where_column << " key " << column_key << std::endl;
  }
  packge.size = Get(select_column, where_column, column_key.c_str(), column_key_len, res, false);
  if (packge.size > 0) {
    int dataSize = 0;
    if(select_column == Id || select_column == Salary) dataSize = packge.size * 8;
    if(select_column == Userid || select_column == Name) dataSize = packge.size * 128;
    packge.data = std::string(res, dataSize);
    std::cout << "Result Size = " << packge.size << "Value = " << packge.data << std::endl;
  }
  return packge;
}


static void deinitNvmDB() {

}