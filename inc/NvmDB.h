#pragma once
#include <cstdint>
#include <cstdio>
#include "./network/Group.h"
#include "./store/NvmStore.h"
#include "util.h"
#include "spdlog/spdlog.h"

// 等价于客户端read的调用次数
static std::atomic<uint32_t> pk_local_count(0);
static std::atomic<uint32_t> uk_local_count(0);
static std::atomic<uint32_t> sk_local_count(0);
// 等价于客户端的read中，无法仅仅通过本地，需要远程的read的次数
static std::atomic<uint32_t> pk_remote_count(0);
static std::atomic<uint32_t> uk_remote_count(0);
static std::atomic<uint32_t> sk_remote_count(0);

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
    static thread_local int write_count = 0;
    write_count++;
    if (write_count > PER_THREAD_MAX_WRITE) {
      spdlog::error("write_count overflow!");
    }
    writeTuple(tuple, len, tid);
    insert(tuple, len, tid);
    // note: write_count just used for log/debug
    if (write_count % 100000 == 0) {
      if (write_count % 1000000 == 0) {
        Util::print_resident_set_size();
      }
      spdlog::info("thread {} write {}", tid, write_count);
    }
}

static std::atomic<uint8_t> getTid(0);
static size_t Get(int32_t select_column,
          int32_t where_column, const void *column_key, size_t column_key_len, void *res, bool is_local){
    static thread_local uint8_t tid = 0;
    // 1. 设置tid
    if (is_local == true && tid == 0) { // socket_server 也会调用该函数，防止tid溢出
      tid = getTid++;
      if (tid >= 50) {
        if (tid >= SYNC_TID) {
          spdlog::error("[Get] tid overflow, tid = {}", tid);
          exit(1);
        }
        mustAddConnect(tid);
      }
    }
    // 2. 输出一些log来debug
    static thread_local int local_read_count = 0;
    static thread_local int remote_read_count = 0;
    if (is_local) {
      if (where_column == 0) pk_local_count++;
      if (where_column == 1) uk_local_count++;
      if (where_column == 3) sk_local_count++;
      local_read_count++;
      if (local_read_count == 1) {
        spdlog::info("first call local_read_count once");
      }
      if (local_read_count % 500000 == 0) {
        spdlog::info("local_read_count {}", local_read_count);
      }
    } else {
      remote_read_count++;
      if (remote_read_count == 1) {
        spdlog::info("first call remote_read_count once");
      }
      if (remote_read_count % 500000 == 0) {
        spdlog::info("remote_read_count {}", remote_read_count);
      }
    }
    // 3. 尝试从本地读
    std::vector<uint32_t> posArray = getPosFromKey(where_column, column_key, is_local);
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
    // 4. 从本地读不到，则从远端读。对于salary列，即使本地读到了，也要尝试从远端读
    if ((posArray.size() == 0 || where_column == Salary) && is_local) {
      if (where_column == 0) pk_remote_count++;
      if (where_column == 1) uk_remote_count++;
      if (where_column == 3) sk_remote_count++;
      Package result;
      if (where_column == 1) {
        char hash_colum_key[8];
        UserId uid = UserId((char *)column_key);
        memcpy(hash_colum_key, &uid.hashCode1, 4);
        memcpy(hash_colum_key + 4, &uid.hashCode2, 4);
        result = clientRemoteGet(select_column, where_column, hash_colum_key, 8, tid);
      } else {
        result = clientRemoteGet(select_column, where_column, column_key, column_key_len, tid);
      }
      int dataSize = 0;
      if(select_column == Id || select_column == Salary) dataSize = result.size * 8;
      if(select_column == Userid || select_column == Name) dataSize = result.size * 128;      
      memcpy(res, result.data, dataSize);
      return result.size + posArray.size(); 
    }
    return posArray.size();
}

static Package remoteGet(int32_t select_column,
          int32_t where_column, char *column_key, size_t column_key_len) {
  Package packge;
  uint64_t key = *(uint64_t *)(column_key);
  spdlog::debug("Remote Get Select {} where {} key {}", select_column, where_column, key);

  packge.size = Get(select_column, where_column, column_key, column_key_len, packge.data, false);
  if (packge.size > 0) {
    int dataSize = 0;
    if (select_column == Salary || select_column == Id) {
      spdlog::debug("Result Size = {}, Value = {}", packge.size, *(uint64_t *)packge.data);
    } else {
      spdlog::debug("Result Size = {}, Value = {}", packge.size, packge.data);
    }
  }
  return packge;
}

static void deinitNvmDB() {
  spdlog::info("NvmDB ready to deinit");
  deInitGroup();
  spdlog::info("Server local get pk {}", pk_local_count);
  spdlog::info("Server local get uk {}", uk_local_count);
  spdlog::info("Server local get sk {}", sk_local_count);
  spdlog::info("Server remote get pk {}", pk_remote_count);
  spdlog::info("Server remote get uk {}", uk_remote_count);
  spdlog::info("Server remote get sk {}", sk_remote_count);
  spdlog::info("NvmDB deinit done");
}
