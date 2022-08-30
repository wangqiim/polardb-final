#pragma once
#include <cstdint>
#include <cstdio>
#include "./network/Group.h"
#include "./store/NvmStore.h"
#include "util.h"
#include "spdlog/spdlog.h"

static void initNvmDB(const char* host_info, const char* const* peer_host_info, size_t peer_host_info_num,
                const char* aep_dir, const char* disk_dir){
    spdlog::info("[initNvmDB] NvmDB Init Begin");
    initIndex();
    initStore(aep_dir, disk_dir);
    initGroup(host_info, peer_host_info, peer_host_info_num);
    Util::print_resident_set_size();
    spdlog::info("[initNvmDB] NvmDB Init END");
}

static std::atomic<uint8_t> putTid(0);
static void Put(const char *tuple, size_t len){
    static thread_local uint8_t tid = putTid++;
    if (tid >= PMEM_FILE_COUNT) {
      spdlog::error("tid overflow, current tid = {}", tid);
    }
    writeTuple(tuple, len, tid);
    insert(tuple, len, tid);
    // note: write_count just used for log/debug
    static thread_local int write_count = 0;
    write_count++;
    if (write_count % 1000000 == 0) {
      spdlog::debug("thread {} write {}", tid, write_count);
    }
}

static size_t Get(int32_t select_column,
          int32_t where_column, const void *column_key, size_t column_key_len, void *res, bool is_local){
    std::vector<uint32_t> posArray = getPosFromKey(where_column, column_key);
    uint32_t result_bytes = 0;
    if (posArray.size() > 0){
      for (uint32_t pos : posArray) {
        readColumFromPos(select_column, pos, res);
        if(select_column == Id || select_column == Salary) {
          result_bytes += 8;
          res += 8;
        } 
        if(select_column == Userid || select_column == Name) {
          result_bytes += 128;
          res += 128;
        }
        if (result_bytes >= 2000 * 8) {
          spdlog::error("result overflow!!!!!!");
          exit(1);
        }
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
    return posArray.size();
}

static Package remoteGet(rpc_conn conn, int32_t select_column,
          int32_t where_column, const std::string &column_key, size_t column_key_len) {
  char res[2000 * 8];
  Package packge;
  if (where_column == Salary || where_column == Id) {
    uint64_t key = *(uint64_t *)(column_key.c_str());
    spdlog::debug("Remote Get Select {} where {} key {}", select_column, where_column, key);
  } else {
    spdlog::debug("Remote Get Select {} where {} key {}", select_column, where_column, column_key);
  }
  packge.size = Get(select_column, where_column, column_key.c_str(), column_key_len, res, false);
  if (packge.size > 0) {
    int dataSize = 0;
    if(select_column == Id || select_column == Salary) dataSize = packge.size * 8;
    if(select_column == Userid || select_column == Name) dataSize = packge.size * 128;
    packge.data = std::string(res, dataSize);
    if (select_column == Salary || select_column == Id) {
      spdlog::debug("Result Size = {}, Value = {}", packge.size, *(uint64_t *)packge.data.c_str());
    } else {
      spdlog::debug("Result Size = {}, Value = {}", packge.size, packge.data);
    }
  }
  return packge;
}


static void deinitNvmDB() {
  spdlog::info("NvmDB ready to deinit");
  deInitGroup();

  spdlog::info("NvmDB deinit done");
}
