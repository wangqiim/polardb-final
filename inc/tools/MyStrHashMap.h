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

/**
 * value的存储格式(uint32_t),从左到右，对应高位到低位
 * -------------------------------------------------------------------------------------------------------------------------
 * |            第31位             |                         第29~30位                     |   第28位  | 第0~27位            |
 * -------------------------------------------------------------------------------------------------------------------------
 * | 0: normal数据区，1: rand数据区 | 00: 对应本地, 01、10、11: 对应远端是哪一个peer_idx(1~3) |   未使用  | position(0 ~ 2亿-1) |
 * -------------------------------------------------------------------------------------------------------------------------
 * 
*/
class MySalaryHashMap {
  public:
  static uint32_t peeridx_offset(uint32_t peer_idx, uint32_t id) {
    // (第29~30位) | (第0~27位)
    return ((peer_idx + 1) << 29) | (id - peer_offset[peer_idx]);
  } 

  static uint32_t Pos2Id(uint32_t pos) {
    uint32_t peer = (pos & 0x60000000U) >> 29; // 掩码第29~30位
    uint32_t rel_pos = pos & 0x0FFFFFFFU; // 掩码低28位
    return rel_pos + peer_offset[peer - 1];
  }

  // peer_idx值为0,1,2
  static uint32_t Pos2Peer_idx(uint32_t pos) { return ((pos & 0x60000000U) >> 29) - 1; }

  static bool is_local(uint32_t pos) { return (pos & 0x60000000U) == 0; } // 第29~30位是00

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
      spdlog::error("[MySalaryHashMap] pmem_map_file");
    }
    pmem_memset_nodrain(pmem_addr_, 0, mmap_size);
  }

  ~MySalaryHashMap() {
    delete hash_table;
  }

  // note(wq): 对存在本地上的数据，调用该接口是无意义的!!!(结果错误)
  // todo(wq): 如果需要调用该接口能获取本地的id，则需要添加逻辑处理偏移，以及rand pmem区域上的数据。
  bool get_id(uint64_t key, uint64_t *id) {
    uint32_t bucket_idx = key & (hashSize_ - 1);
    if (hash_table[bucket_idx].value == 0) return false;
    uint32_t pos = hash_table[bucket_idx].value - 1;
    *id = Pos2Id(pos);
    return true;
  }

 void get(uint64_t key, std::vector<uint32_t> &ans, bool *need_remote_peers) {
  // todo(wq): implement me
    uint32_t pos = key & (hashSize_ - 1);
    if (hash_table[pos].value == 0) return;
    else {
      if (is_local(pos)) {
        ans.push_back(hash_table[pos].value - 1);        
      } else {
        need_remote_peers[Pos2Peer_idx(pos)] = true;
      }
      if (pmem_record_num_ != 0) {
        // todo(wq): 性能阶段增加冲突处理
        std::lock_guard<std::mutex> guard(mtx);
        for (uint64_t i = 0; i < pmem_record_num_; i++) {
          kv_pair temp = *(kv_pair *)(pmem_addr_ + i * sizeof(kv_pair));
          if (key == temp.first) {
            ans.push_back(temp.second);
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
      kv_pair temp = {key ,value};
      pmem_memcpy_nodrain(pmem_addr_ + pmem_record_num_ * sizeof(kv_pair), &temp, sizeof(kv_pair));
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

class MyUserIdHashMap {
  public:
  MyUserIdHashMap(uint32_t hashSize, std::string file_name) {
    hash_table = new MyStrHead[hashSize];
    hashSize_ = hashSize;
    pmem_record_num_ = 0;
    uint64_t mmap_size = TOTAL_WRITE_NUM * 4;
    int is_pmem;
    size_t mapped_len;
    file_name = "/mnt/aep/" + file_name;
    if ( (pmem_addr_ = (char *)pmem_map_file(file_name.c_str(), mmap_size, PMEM_FILE_CREATE,
                                                  0666, &mapped_len, &is_pmem)) == NULL ) {
      spdlog::error("[MyUserIdHashMap] pmem_map_file");
    }
    pmem_memset_nodrain(pmem_addr_, 0, mmap_size);
  }

  ~MyUserIdHashMap() {
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

class MyString256HashMap {
  public:
  MyString256HashMap() {
    hash_table = new MyStr256Head[hashSize];
  }

  ~MyString256HashMap() {
    delete hash_table;
  }

  char * get(uint32_t key) {
    return hash_table[key & (hashSize - 1)].data;
  }

  void insert(uint32_t key, const char * value) {
    memcpy(hash_table[key & (hashSize - 1)].data, value, 256);
  }

  void stat() {
    // std::cout << "recovery boom " << hash_boom << std::endl;
  }

  private:
  MyStr256Head *hash_table;
  const uint32_t hashSize = 1<<26;
};

class MyPKHashMap {
  public:
  MyPKHashMap() {
    hash_table = new uint32_t[hashSize];
    memset(hash_table, 0, sizeof(uint32_t) * hashSize);
    salary_table = new uint64_t[hashSize];
    memset(salary_table, 0, sizeof(uint64_t) * hashSize);
  }

  ~MyPKHashMap() {
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
    uint32_t bucket_idx = key & (hashSize - 1);
    if (salary_table[bucket_idx] == 0) {
      salary_table[bucket_idx] = salary + 1;
    }
  }

  uint64_t get_salary(uint64_t key) {
    return salary_table[key & (hashSize - 1)]; //如果返回值是 0,表示是空的
  }

  private:
  uint32_t *hash_table;
  uint64_t *salary_table;
  const uint32_t hashSize = 1<<30;
};