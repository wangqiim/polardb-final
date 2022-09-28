#pragma once
#include <cstdint>
#include <cstdio>
#include "./network/Group.h"
#include "./store/NvmStore.h"
#include "util.h"
#include "spdlog/spdlog.h"
#include <chrono>
#include <thread>

#ifdef debug_db
// 等价于客户端read的调用次数
static std::atomic<uint32_t> pk_local_count(0);
static std::atomic<uint32_t> uk_local_count(0);
static std::atomic<uint32_t> sk_local_count(0);
// 等价于客户端的read中，无法仅仅通过本地，需要远程的read的次数
static std::atomic<uint32_t> pk_remote_count(0);
static std::atomic<uint32_t> uk_remote_count(0);
static std::atomic<uint32_t> sk_remote_count(0);
static std::atomic<uint32_t> sk_local_hit_remote_hit(0);
static std::atomic<uint32_t> sk_local_hit_remote_miss(0);
static std::atomic<uint32_t> sk_local_miss_remote_hit(0);
static std::atomic<uint32_t> sk_local_miss_remote_miss(0);
static std::atomic<uint32_t> sk_remote_select[4] = {0, 0, 0, 0};

static uint32_t sk_broadcast_3_peer_count[MAX_Client_Num];

void stat_log() {
  spdlog::info("Server local get pk {}", pk_local_count);
  spdlog::info("Server local get uk {}", uk_local_count);
  spdlog::info("Server local get sk {}", sk_local_count);
  spdlog::info("Server remote get pk {}", pk_remote_count);
  spdlog::info("Server remote get uk {}", uk_remote_count);
  spdlog::info("Server remote get sk {}", sk_remote_count);
  uint64_t total_pk_remote_success = 0, total_uk_remote_success = 0, total_sk_remote_success = 0;
  for (int i = 0; i < 3; i++) {
    spdlog::info("clinet {} remote_pk_success_cnt {}", i, remote_pk_success_cnt[i]);
    spdlog::info("clinet {} remote_uk_success_cnt {}", i, remote_uk_success_cnt[i]);
    spdlog::info("clinet {} remote_sk_success_cnt {}", i, remote_sk_success_cnt[i]);
    total_pk_remote_success += remote_pk_success_cnt[i];
    total_uk_remote_success += remote_uk_success_cnt[i];
    total_sk_remote_success += remote_sk_success_cnt[i];
  }
  spdlog::info("total_pk_remote_success {}", total_pk_remote_success);
  spdlog::info("total_uk_remote_success {}", total_uk_remote_success);
  spdlog::info("total_sk_remote_success {}", total_sk_remote_success);
  spdlog::info("------------sk stats--------------");
  spdlog::info("sk_local_hit_remote_hit {}", sk_local_hit_remote_hit);
  spdlog::info("sk_local_hit_remote_miss {}", sk_local_hit_remote_miss);
  spdlog::info("sk_local_miss_remote_hit {}", sk_local_miss_remote_hit);
  spdlog::info("sk_local_miss_remote_miss {}", sk_local_miss_remote_miss);
  for (int i = 0; i < 4; i++) {
    spdlog::info("[where sk] success remote select_column = {}, count = {}", i, sk_remote_select[i]);
  }
  uint64_t total_sk_broadcast_3_peers_count = 0;
  for (int tid = 0; tid < MAX_Client_Num; tid++) {
    total_sk_broadcast_3_peers_count += sk_broadcast_3_peer_count[tid];
  }
  spdlog::info("total_sk_broadcast_3_peers_count = {}", total_sk_broadcast_3_peers_count);
  // sk.stat();
}

#endif

std::mutex finished_mtx;
std::condition_variable finished_cv;
static int finished_write_thread_cnt = 0;

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
      spdlog::error("[Put] tid overflow, current tid = {}", tid);
    }
    static thread_local uint64_t write_count = 0;
    write_count++;
    if (write_count > PER_THREAD_MAX_WRITE) {
      spdlog::error("write_count overflow!");
    }
    writeTuple(tuple, len, tid);
    insert(tuple, len, tid);
    
    spdlog::debug("[Put] local write salary: {}", *(uint64_t *)(tuple + 264));
    if(write_count % salary_page_cnt == 0) {
      broadcast_salary(MBM[tid].address + (write_count - salary_page_cnt) * 16, tid);
    }
//    if(write_count % 100000 == 0)
//      spdlog::info("[Put] local write {}", write_count);
    if (write_count == 200000) is_use_remote_pk = true;
    if (write_count == PER_THREAD_MAX_WRITE) {
      std::unique_lock lk(finished_mtx);
      if (++finished_write_thread_cnt != 50) {
        finished_cv.wait(lk); // 兜底可以用wait_for保证正确性
      } else {
        Store_Sync();
        Util::print_resident_set_size();
        spdlog::info("total write 200000000 tuples");
#ifdef debug_db
        stat_log();
#endif
        uint64_t tmp_max = 0, tmp_min = 0xFFFFFFFFFFFFFFFF;
        for (int i = 0; i < 50; i++) {
          if (local_max_pks[i] > tmp_max) tmp_max = local_max_pks[i];
          if (local_min_pks[i] < tmp_min) tmp_min = local_min_pks[i];
        }
        local_max_pk = tmp_max;
        local_min_pk = tmp_min;
        // using namespace std::chrono_literals;
        // std::this_thread::sleep_for(300s);
        // spdlog::info("wait for salary cache replay");
        // while (finish_sync_cnt != Salary_Cache_Num) {
        //   using namespace std::chrono_literals;
        //   std::this_thread::sleep_for(10ms);
        // }
        // spdlog::info("salary cache replay done!");
        is_use_remote_pk = true;
        finished_cv.notify_all();
      }
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
        if (tid >= MAX_Client_Num) {
          spdlog::error("[Get] tid overflow, tid = {}", tid);
          exit(1);
        }
        mustAddConnect(tid);
      }
    }
    // 2. 输出一些log来debug
#ifdef debug_db
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
#endif
    // 3. 尝试从本地读
    size_t local_get_count = 0;
//    if (where_column == 1 && (select_column == 0 || select_column == 3)) {
//        local_get_count = getValueFromUK(select_column, column_key, is_local, res);
//    } else {
        bool need_remote_peers[3] = {true, true, true}; // 目前只控制salary
        static thread_local std::vector<uint32_t> posArray;
        posArray.clear();
        getPosFromKey(posArray, where_column, column_key, is_local, need_remote_peers);
        uint32_t result_bytes = 0;
        if (posArray.size() > 0) {
            for (uint32_t pos: posArray) {
                readColumFromPos(select_column, pos, res);
                if (select_column == Id || select_column == Salary) {
                    result_bytes += 8;
                    res = (char *) res + 8;
                }
                if (select_column == Userid || select_column == Name) {
                    result_bytes += 128;
                    res = (char *) res + 128;
                }
                if (result_bytes >= PACKAGE_DATA_SIZE) {
                    spdlog::error("result overflow!!!!!!");
                    exit(1);
                }
            }
            if (where_column != Salary || is_use_remote_pk) return posArray.size();
        }
        local_get_count = posArray.size();

    // 4. 从本地读不到，则从远端读。对于salary列，即使本地读到了，也要尝试从远端读
    if ((local_get_count == 0 || where_column == Salary) && is_local) {
#ifdef debug_db
      if (where_column == 0) pk_remote_count++;
      if (where_column == 1) uk_remote_count++;
      if (where_column == 3) sk_remote_count++;
#endif
      Package result;
      if (where_column == 1) {
        char hash_colum_key[8];
        UserId uid = UserId((char *)column_key);
        memcpy(hash_colum_key, &uid.hashCode, 8);
        result = clientRemoteGet(select_column, where_column, hash_colum_key, 8, tid, need_remote_peers);
      } else {
        bool is_find = false;
        if (where_column == 3 && select_column == 0 && is_use_remote_pk) {
           get_remote_id_from_salary(*(uint64_t *)column_key, result.data, is_find);
        }
        if (is_find) {
          result.size = 1;
        } else {
#ifdef debug_db
          if (where_column == Salary && need_remote_peers[0] && need_remote_peers[1] && need_remote_peers[2]) {
            sk_broadcast_3_peer_count[tid]++;
          }
#endif
          result = clientRemoteGet(select_column, where_column, column_key, column_key_len, tid, need_remote_peers);
        }
      }
      int dataSize = 0;
      if(select_column == Id || select_column == Salary) dataSize = result.size * 8;
      if(select_column == Userid || select_column == Name) dataSize = result.size * 128;      
      memcpy(res, result.data, dataSize);
#ifdef debug_db
      if (where_column == Salary) {
        if (local_get_count > 0) {
          if (result.size > 0) {
            sk_local_hit_remote_hit++;
          } else {
            sk_local_hit_remote_miss++;
          }
        } else {
          if (result.size > 0) {
            sk_remote_select[select_column]++;
            sk_local_miss_remote_hit++;
          } else {
            sk_local_miss_remote_miss++;
          }
        }
      }
#endif
      return result.size + local_get_count;
    }
    return local_get_count;
}

static Package remoteGet(int32_t select_column,
          int32_t where_column, char *column_key, size_t column_key_len) {
  Package packge;
  uint64_t key = *(uint64_t *)(column_key);
  spdlog::debug("Remote Get Select {} where {} key {}", select_column, where_column, key);

  packge.size = Get(select_column, where_column, column_key, column_key_len, packge.data, false);
  if (packge.size > 0) {
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
#ifdef debug_db
  stat_log();
#endif
  store_stat();
  spdlog::info("NvmDB deinit done");
}
