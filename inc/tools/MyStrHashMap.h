#pragma once
#include <iostream>
#include <sys/mman.h>
#include <xmmintrin.h>
#include <unistd.h> 
#include <string.h>
#include <vector>
#include <mutex>
#include <libpmem.h>
#include "../Config.h"

struct alignas(64) MyStr256Head
{
  char data[256];
};

struct alignas(4) MyStrHead {
    uint32_t value = 0;
};

class MySalaryHashMap {
  public:
  // 高2位. 01,10,11分别对应3个peerid, 低30位对应id值(1-8亿) 高2位00对应本地写
  static uint32_t peer_idx2Pos(int peer_idx, uint32_t id) { return (((uint32_t(peer_idx) + 1) << 30) & 0xC0000000UL) | id; } 
  static uint32_t Pos2Id(uint32_t pos) { return pos & 0x3FFFFFFFUL; } // 低30位放id
  static uint32_t Pos2Peer_idx(uint32_t pos) { return ((pos & 0xC0000000UL) >> 30) - 1; }
  static bool is_local(uint32_t pos) { return (pos & 0xC0000000UL) == 0; } // 高2位是00

  typedef std::pair<uint64_t, uint32_t> kv_pair; // 16 bytes
    MySalaryHashMap(uint32_t hashSize, std::string file_name) {
    hash_table = new MyStrHead[hashSize];
    hashSize_ = hashSize;
    pmem_record_num_ = 0;
    uint64_t mmap_size = TOTAL_WRITE_NUM * sizeof(kv_pair);
    int is_pmem;
    size_t mapped_len;
    file_name = "/mnt/aep/" + file_name;
    if ( (pmem_addr_ = (char *)pmem_map_file(file_name.c_str(), mmap_size, PMEM_FILE_CREATE,
                                                  0666, &mapped_len, &is_pmem)) == NULL ) {
      spdlog::error("[MyStringHashMap] pmem_map_file");
    }
    pmem_memset_nodrain(pmem_addr_, 0, mmap_size);
  }

  ~MySalaryHashMap() {
    delete hash_table;
  }

  uint64_t get_id(uint64_t key) {
    uint32_t bucket_idx = key & (hashSize_ - 1);
    if (hash_table[bucket_idx].value == 0) return 0;
    uint32_t pos = hash_table[bucket_idx].value - 1;
    uint64_t id = Pos2Id(pos);
    return id;

    }

 void get(uint64_t key, std::vector<uint32_t> &ans, bool *need_remote_peers) {
    uint32_t bucket_idx = key & (hashSize_ - 1);
    if (hash_table[bucket_idx].value == 0) return;
    else {
      uint32_t pos = hash_table[bucket_idx].value - 1;
      if (is_local(pos)) {
        ans.push_back(pos);
      } else {
        for(uint32_t i = 0; i < 3; i++) {
          if (Pos2Peer_idx(pos) != i) need_remote_peers[i] = false;
        }
      }
      if (pmem_record_num_ != 0) {
        std::lock_guard<std::mutex> guard(mtx);
        for (uint64_t i = 0; i < pmem_record_num_; i++) {
          kv_pair temp = *(kv_pair *)(pmem_addr_ + i * sizeof(kv_pair));
          if (key == temp.first) {
            uint32_t pos = temp.second;
            if (is_local(pos)) {
              ans.push_back(pos);
            } else {
              for(uint32_t j = 0; j < 3; j++) {
                if (Pos2Peer_idx(pos) != j) need_remote_peers[j] = false;
              }
            }
          }
        }
      }
    }
  }

  void insert(uint64_t key, uint32_t value) {
    uint32_t bucket_idx = key & (hashSize_ - 1);
    if (hash_table[bucket_idx].value == 0) {
      hash_table[bucket_idx].value = value + 1;
      return;
      // uint32_t expect_value = 0;
      // auto value_ptr = (std::atomic<uint32_t> *)(&hash_table[bucket_idx].value);
      // if (value_ptr->compare_exchange_strong(expect_value, value + 1)) {
      //   return;
      // }
    }
    std::lock_guard<std::mutex> guard(mtx);
    kv_pair temp = {key ,value};
    pmem_memcpy_nodrain(pmem_addr_ + pmem_record_num_ * sizeof(kv_pair), &temp, sizeof(kv_pair));
    pmem_record_num_++;
  }

  void stat() {
    uint32_t inplace_value_cnt = 0;
    uint32_t local_value_cnt = 0; // 本节点的数据有多少
    uint32_t peers_cnt[3] = {0, 0, 0}; // 三个节点的数据分别是多少
    for (uint32_t i = 0; i < hashSize_; i++) {
      if (hash_table[i].value != 0) {
        inplace_value_cnt++;
        uint32_t pos = hash_table[i].value - 1;
        if (is_local(pos)) {
          local_value_cnt++;
        } else {
          if (Pos2Peer_idx(pos) != 0 && Pos2Peer_idx(pos) != 1 && Pos2Peer_idx(pos) != 2) {
            spdlog::error("[sk stat] invalid Pos2Peer_idx(pos) = {}", Pos2Peer_idx(pos));
            exit(1);
          }
          peers_cnt[Pos2Peer_idx(pos)]++;
        }
      }
    }
    spdlog::info("[sk stat] collision_num = {}", collision_num());
    spdlog::info("[sk stat] inplace_value_cnt = {}", inplace_value_cnt);
    spdlog::info("[sk stat] local_value_cnt = {}, peers_cnts = {}, {}, {}", local_value_cnt, peers_cnt[0], peers_cnt[1], peers_cnt[2]);
  }

  uint64_t collision_num() { return pmem_record_num_; }

  private:
    MyStrHead *hash_table;
    uint32_t hashSize_ = 1<<30;
    
    std::mutex mtx;
    char *pmem_addr_;
    uint64_t pmem_record_num_;
};

class MyUserIdsHashMap {
  public:
  MyUserIdsHashMap(uint32_t hashSize, std::string file_name) {
    hash_table = new MyStrHead[hashSize];
    hashSize_ = hashSize;
    pmem_record_num_ = 0;
    uint64_t mmap_size = TOTAL_WRITE_NUM * 4;
    int is_pmem;
    size_t mapped_len;
    file_name = "/mnt/aep/" + file_name;
    if ( (pmem_addr_ = (char *)pmem_map_file(file_name.c_str(), mmap_size, PMEM_FILE_CREATE,
                                                  0666, &mapped_len, &is_pmem)) == NULL ) {
      spdlog::error("[MyStringHashMap] pmem_map_file");
    }
    pmem_memset_nodrain(pmem_addr_, 0, mmap_size);
  }

  ~MyUserIdsHashMap() {
    delete hash_table;
  }

  // 传入user_id用来对比
  void get(uint64_t key, std::vector<uint32_t> &ans, const char *used_id) {
    uint32_t bucket_idx = key & (hashSize_ - 1);
    if (hash_table[bucket_idx].value == 0) return;
    else {
      uint32_t pos = hash_table[bucket_idx].value - 1;
      // pmem_record_num_ == 0时候，不比user_id了。假设远程读无时，远端节点不会数据碰撞。
      if (pmem_record_num_ == 0 || memcmp(GetUserIdByPos(pos), used_id, 128) == 0) {
        ans.push_back(pos);
        return; //只会有一条符合要求
      }
      if (pmem_record_num_ != 0) {
        std::lock_guard<std::mutex> guard(mtx);
        for (uint64_t i = 0; i < pmem_record_num_; i++) {
          pos = *(uint32_t *)(pmem_addr_ + i * 4);
          if (memcmp(GetUserIdByPos(pos), used_id, 128) == 0) {
            ans.push_back(pos);
            return; //只会有一条符合要求
          }
        }
      }
    }
  }

  void insert(uint64_t key, uint32_t value) {
    uint32_t pos = key & (hashSize_ - 1);
    if (hash_table[pos].value == 0) {
      hash_table[pos].value = value + 1;
    } else {
      std::lock_guard<std::mutex> guard(mtx);
      pmem_memcpy_nodrain(pmem_addr_ + pmem_record_num_ * 4, &value, 4);
      pmem_record_num_++;
    }
  }

  void stat() {
    // std::cout << "recovery boom " << hash_boom << std::endl;
  }

  private:
    MyStrHead *hash_table;
    uint32_t hashSize_ = 1<<30;
    
    std::mutex mtx;
    char *pmem_addr_;
    uint64_t pmem_record_num_;
};

class MyUInt64HashMap {
  public:
  MyUInt64HashMap() {
    hash_table = new uint32_t[hashSize];
    memset(hash_table, 0, sizeof(uint32_t) * hashSize);
    salary_table = new uint64_t [hashSize];
    memset(salary_table, 0, sizeof(uint64_t) * hashSize);
  }

  ~MyUInt64HashMap() {
    delete hash_table;
    delete salary_table;
  }
  //todo 缺少冲突管理
  uint32_t get(uint64_t key) {
    return hash_table[key & (hashSize - 1)]; //如果返回值是 0,表示是空的
  }
  //todo 缺少冲突管理
  void insert(uint64_t key, uint32_t value) {
    hash_table[key & (hashSize - 1)] = value + 1; //不可能出现0 
  }

  void insert_salary(uint32_t key, uint64_t salary) {
    if (key > 0) {
      uint32_t bucket_idx = key & (hashSize - 1);
      if (salary_table[bucket_idx] == 0) {
        salary_table[bucket_idx] = salary;
      }
    }
  }

  uint64_t get_salary(uint64_t key) {
    return salary_table[key & (hashSize - 1)];
  }

  private:
  uint32_t *hash_table;
  uint64_t *salary_table;
  const uint32_t hashSize = 1<<30;
};