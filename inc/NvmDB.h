#pragma once
#include <cstdint>
#include <cstdio>
#include "./network/Group.h"
#include "./store/NvmStoreV2.h"
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
static std::atomic<uint32_t> get_remote_salaryFromPK_fail(0);
static std::atomic<uint32_t> get_remote_idFromSK_fail(0);
static std::atomic<uint32_t> sk_broadcast_after_sync(0);

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
  spdlog::info("local_offset = {}, ip: {}", id_range.first, global_local_host_info);
  for (size_t i = 0; i < 3; i++) {
    spdlog::info("peer_offset[{}] = {}, ip: {}", i, peer_offset[i], global_peer_host_info[i]);
  }
  spdlog::info("get_remote_salaryFromPK_fail = {}", get_remote_salaryFromPK_fail); // 期望是0
  spdlog::info("get_remote_idFromSK_fail = {}", get_remote_idFromSK_fail); // 期望是0
  spdlog::info("sk_broadcast_after_sync = {}", sk_broadcast_after_sync); // 期望是0
}

#endif

std::mutex finished_mtx;
std::condition_variable finished_cv;
static int finished_write_thread_cnt = 0;

// --------------- background salary broadcast -------------------
std::atomic<uint64_t> sync_write_count[PMEM_FILE_COUNT];
std::thread bg_salary_broadcast_th;

void bg_salary_broadcast() {
  uint64_t has_send_num = 0;
  while (has_send_num != PER_THREAD_MAX_WRITE) {
    uint64_t min_send_salary_num = PER_THREAD_MAX_WRITE;
    for (size_t tid = 0; tid < PMEM_FILE_COUNT; tid++) {
      min_send_salary_num = std::min(min_send_salary_num, sync_write_count[tid].load());
    }
    if (has_send_num != min_send_salary_num) {
      uint64_t cur_send_sarlay_num = (min_send_salary_num - has_send_num) * PMEM_FILE_COUNT;
      broadcast_salary(has_send_num * PMEM_FILE_COUNT, cur_send_sarlay_num);
      has_send_num = min_send_salary_num;
      spdlog::info("[bg_salary_broadcast] has_send_num = {}", has_send_num);
    }
    std::this_thread::sleep_for(std::chrono::milliseconds(100));
  }
  spdlog::info("[bg_salary_broadcast] finish");
  sync();
}

std::thread bg_heartbeat_th;
void bg_heartbeat() {
  while (true) {
    if (is_sync_all) { // 性能读阶段才开始打印
      spdlog::info("[bg_heartbeat] total_read_count = {}", pk_local_count + uk_local_count + sk_local_count);
    }
    std::this_thread::sleep_for(std::chrono::seconds(1));
  }
}

static void initNvmDB(const char* host_info, const char* const* peer_host_info, size_t peer_host_info_num,
                const char* aep_dir, const char* disk_dir){
    spdlog::info("[initNvmDB] NvmDB Init Begin");
    initIndex();
    initStore(aep_dir, disk_dir);
    initGroup(host_info, peer_host_info, peer_host_info_num);
    bg_salary_broadcast_th = std::thread(bg_salary_broadcast);
    bg_heartbeat_th = std::thread(bg_heartbeat);
    Util::print_resident_set_size();
    spdlog::info("[initNvmDB] NvmDB Init END");
}

static std::atomic<uint8_t> putTid(0);
static void Put(const char *tuple, size_t len) {
    static thread_local uint8_t tid = putTid++;
    // _mm_prefetch(tuple, _MM_HINT_T0); // todo(wq): may it is useless
    static thread_local uint64_t write_count = 0;
    writeTuple(tuple, len);
    write_count++;
    if (write_count % 100 == 0) {
      sync_write_count[tid].store(write_count);
    }
    if (write_count == PER_THREAD_MAX_WRITE) {
      std::unique_lock lk(finished_mtx);
      if (++finished_write_thread_cnt != 50) {
        finished_cv.wait(lk); // 兜底可以用wait_for保证正确性
      } else {
        spdlog::info("total write 200000000 tuples");
//        Store_Sync(false); // true: async, false: sync
        spdlog::info("Store_Sync finish");
        Util::print_resident_set_size();
#ifdef debug_db
        stat_log();
#endif
        local_min_pk = id_range.first;
        local_max_pk = id_range.second - 1;
        spdlog::info("local_min_pk = {}, local_max_pk = {}", local_min_pk, local_max_pk);
        is_sync_all = true;

        spdlog::info("ready salary_sync finish.....");
        std::unique_lock salary_sync_lk(salary_sync_cnt_mtx);
        if (salary_sync_cnt != Salary_Cache_Num) {
          salary_sync_cnt_cv.wait(salary_sync_lk);
        }
        spdlog::info("salary_sync finish!!!");
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
        spdlog::debug("first call local_read_count once");
      }
      if (local_read_count % 1000000 == 0) {
        spdlog::info("local_read_count {}", local_read_count);
      }
    } else {
      remote_read_count++;
      if (remote_read_count == 1) {
        spdlog::debug("first call remote_read_count once");
      }
      if (remote_read_count % 1000000 == 0) {
        spdlog::info("remote_read_count {}", remote_read_count);
      }
    }
#endif
    // 3. 尝试从本地读
    size_t local_get_count = 0;
//    if (where_column == 1 && (select_column == 0 || select_column == 3)) {
//        local_get_count = getValueFromUK(select_column, column_key, is_local, res);
//    } else {
        bool need_remote_peers[3] = {true, true, true};
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
            if (where_column != Salary || is_sync_all) return posArray.size();
        }
        local_get_count = posArray.size();
//    }
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
        if (is_sync_all && where_column == Id && select_column == Salary) {
          getRemoteSalaryFromPK(*(uint64_t *)column_key, (char *)res, is_find);
#ifdef debug_db
          if (!is_find) {
            get_remote_salaryFromPK_fail++;
          }
#endif
        } else if (is_sync_all && where_column == Salary && select_column == Id) {
          getRemoteIdFromSK(*(uint64_t *)column_key, (char *)res, is_find);
#ifdef debug_db
          if (!is_find) {
            get_remote_idFromSK_fail++;
          }
#endif
        }
        if (is_find) {
          return 1;
        }
#ifdef debug_db
        if (is_sync_all && where_column == Salary && need_remote_peers[0] && need_remote_peers[1] && need_remote_peers[2]) {
          sk_broadcast_after_sync++;
        }
#endif
        result = clientRemoteGet(select_column, where_column, column_key, column_key_len, tid, need_remote_peers);
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
            sk_local_miss_remote_hit++;
          } else {
            sk_local_miss_remote_miss++;
          }
        }
      }
#endif

      // if (result.size + local_get_count == 0) {
      //   spdlog::warn("[Get] local + remote return_num = {}, where_column = {}", result.size + local_get_count, where_column);
      // }
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
  spdlog::info("NvmDB deinit done");
}
