#pragma once
#include <cstdint>
#include <cstdio>
#include <cmath>
#include <algorithm>
#include "./network/Group.h"
#include "./store/NvmStore.h"
#include "util.h"
#include "spdlog/spdlog.h"

std::atomic<uint64_t> read_local_cnt[4][4];
std::atomic<uint64_t> read_remote_cnt[4][4];

thread_local int local_read_count = 0;
thread_local int remote_read_count = 0;

std::atomic<uint64_t> total_write_cnt = {0};

uint64_t local_write_max_pk[50];
uint64_t local_write_min_pk[50];
uint64_t local_write_max_sk[50];
uint64_t local_write_min_sk[50];

uint64_t local_read_max_pk[50]; 
uint64_t local_read_min_pk[50];
uint64_t local_read_max_sk[50];
uint64_t local_read_min_sk[50];

static void initNvmDB(const char* host_info, const char* const* peer_host_info, size_t peer_host_info_num,
                const char* aep_dir, const char* disk_dir){
    {
      for (int i = 0; i < 50; i++) {
        local_write_max_pk[i] = 0;
        local_write_max_sk[i] = 0;
        local_read_max_pk[i] = 0;
        local_read_max_sk[i] = 0;
        local_write_min_pk[i] = UINT64_MAX;
        local_write_min_sk[i] = UINT64_MAX;
        local_read_min_pk[i] = UINT64_MAX;
        local_read_min_sk[i] = UINT64_MAX;
      }
    }
    spdlog::info("[initNvmDB] NvmDB Init Begin");
    initIndex();
    initStore(aep_dir, disk_dir);
    initGroup(host_info, peer_host_info, peer_host_info_num);
    Util::print_resident_set_size();
    spdlog::info("[initNvmDB] NvmDB Init END");
}

static std::atomic<uint8_t> putTid(0);
static void Put(const char *tuple, size_t len){
    total_write_cnt++;
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
    { // 线程不安全的
      uint64_t pk = *(uint64_t *)tuple;
      uint64_t sk = *(uint64_t *)(tuple + 264);
      local_write_max_pk[tid] = std::max(pk, local_write_max_pk[tid]);
      local_write_min_pk[tid] = std::min(pk, local_write_min_pk[tid]);
      local_write_max_sk[tid] = std::max(sk, local_write_max_sk[tid]);
      local_write_min_sk[tid] = std::min(sk, local_write_min_sk[tid]);
    }
    // note: write_count just used for log/debug
    if (write_count % 100000 == 0) {
      if (write_count % 1000000 == 0) {
        Util::print_resident_set_size();
      }
      spdlog::info("thread {} write {}", tid, write_count);
    }
    if (total_write_cnt == 200000000) {
      for (int select_column = 0; select_column < 4; select_column++) {
        for (int where_column = 0; where_column < 4; where_column++) {
          spdlog::info("Server local get select_column: {}, where_column: {}, count: {}", select_column, where_column, read_local_cnt[select_column][where_column]);
          spdlog::info("Server local get select_column: {}, where_column: {}, count: {}", select_column, where_column, read_remote_cnt[select_column][where_column]);
        }
      }
      spdlog::info("thread {} write 200000000th tuples", tid);
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
    if (is_local) {
      read_local_cnt[select_column][where_column]++;
      if (tid < 50) {
        if (where_column == Id) {
          uint64_t pk = *(uint64_t *)(column_key);
          local_read_max_pk[tid] = std::max(pk, local_read_max_pk[tid]);
          local_read_min_pk[tid] = std::min(pk, local_read_min_pk[tid]);
        } else if (where_column == Salary) {
          uint64_t sk = *(uint64_t *)(column_key);
          local_read_max_sk[tid] = std::max(sk, local_read_max_sk[tid]);
          local_read_min_sk[tid] = std::min(sk, local_read_min_sk[tid]);
        }
      }
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
    // 4. 从本地读不到，则从远端读。对于salary列，即使本地读到了，也要尝试从远端读
    if ((posArray.size() == 0 || where_column == Salary) && is_local) {
      read_remote_cnt[select_column][where_column]++;
      Package result = clientRemoteGet(select_column, where_column, column_key, column_key_len, tid);
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
  if (where_column == Salary || where_column == Id) {
    uint64_t key = *(uint64_t *)(column_key);
    spdlog::debug("Remote Get Select {} where {} key {}", select_column, where_column, key);
  } else {
    spdlog::debug("Remote Get Select {} where {} key {}", select_column, where_column, to_hex((unsigned char *)column_key, 128));
  }
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
  spdlog::info("------ log local read type --------");
  for (int select_column = 0; select_column < 4; select_column++) {
    for (int where_column = 0; where_column < 4; where_column++) {
      spdlog::info("Server local get select_column: {}, where_column: {}, count: {}", select_column, where_column, read_local_cnt[select_column][where_column]);
      spdlog::info("Server local get select_column: {}, where_column: {}, count: {}", select_column, where_column, read_remote_cnt[select_column][where_column]);
    }
  }
  spdlog::info("------ log local write max/min pk/sk -------");
  uint64_t total_local_write_max_pk = 0;
  uint64_t total_local_write_min_pk = UINT64_MAX;
  uint64_t total_local_write_max_sk = 0;
  uint64_t total_local_write_min_sk = UINT64_MAX;
  for (int i = 0; i < 50; i++) {
    spdlog::info("local_write_max_pk[{}] = {}", i, local_write_max_pk[i]);
    spdlog::info("local_write_min_pk[{}] = {}", i, local_write_min_pk[i]);
    spdlog::info("local_write_max_sk[{}] = {}", i, local_write_max_sk[i]);
    spdlog::info("local_write_min_sk[{}] = {}", i, local_write_min_sk[i]);
    total_local_write_max_pk = std::max(total_local_write_max_pk, local_write_max_pk[i]);
    total_local_write_min_pk = std::min(total_local_write_min_pk, local_write_min_pk[i]);
    total_local_write_max_sk = std::max(total_local_write_max_sk, local_write_max_sk[i]);
    total_local_write_min_sk = std::min(total_local_write_min_sk, local_write_min_sk[i]);
  }
  spdlog::info("total_local_write_max_pk = {}", total_local_write_max_pk);
  spdlog::info("total_local_write_min_pk = {}", total_local_write_min_pk);
  spdlog::info("total_local_write_max_sk = {}", total_local_write_max_sk);
  spdlog::info("total_local_write_min_sk = {}", total_local_write_min_sk);

  spdlog::info("--------log local read max/min pk/sk----------------");
  uint64_t total_local_read_max_pk = 0;
  uint64_t total_local_read_min_pk = UINT64_MAX;
  uint64_t total_local_read_max_sk = 0;
  uint64_t total_local_read_min_sk = UINT64_MAX;
  for (int i = 0; i < 50; i++) {
    spdlog::info("local_read_max_pk[{}] = {}", i, local_read_max_pk[i]);
    spdlog::info("local_read_min_pk[{}] = {}", i, local_read_min_pk[i]);
    spdlog::info("local_read_max_sk[{}] = {}", i, local_read_max_sk[i]);
    spdlog::info("local_read_min_sk[{}] = {}", i, local_read_min_sk[i]);
    total_local_read_max_pk = std::max(total_local_read_max_pk, local_read_max_pk[i]);
    total_local_read_min_pk = std::min(total_local_read_min_pk, local_read_min_pk[i]);
    total_local_read_max_sk = std::max(total_local_read_max_sk, local_read_max_sk[i]);
    total_local_read_min_sk = std::min(total_local_read_min_sk, local_read_min_sk[i]);
  }
  spdlog::info("total_local_read_max_pk = {}", total_local_read_max_pk);
  spdlog::info("total_local_read_min_pk = {}", total_local_read_min_pk);
  spdlog::info("total_local_read_max_sk = {}", total_local_read_max_sk);
  spdlog::info("total_local_read_min_sk = {}", total_local_read_min_sk);
  spdlog::info("------------------------------------------------------");

  spdlog::info("total_write_cnt = {}", total_write_cnt);

  spdlog::info("NvmDB deinit done");
}
