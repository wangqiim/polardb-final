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

struct alignas(8) MyStrHead {
    uint32_t value = 0;
};

class MyStringHashMap {
  public:
  typedef std::pair<uint64_t, uint32_t> kv_pair; // 16 bytes
  MyStringHashMap() {
    hash_table = new MyStrHead[hashSize];
    pmem_record_num_ = 0;
    uint64_t mmap_size = TOTAL_WRITE_NUM * sizeof(kv_pair);
    int is_pmem;
    size_t mapped_len;
    if ( (pmem_addr_ = (char *)pmem_map_file("/mnt/aep/skindex", mmap_size, PMEM_FILE_CREATE,
                                                  0666, &mapped_len, &is_pmem)) == NULL ) {
      spdlog::error("[MyStringHashMap] pmem_map_file");
    }
    pmem_memset_nodrain(pmem_addr_, 0, mmap_size);
  }

  ~MyStringHashMap() {
    delete hash_table;
  }

 void get(uint64_t key, std::vector<uint32_t> &ans) {
    uint32_t pos = key & (hashSize - 1);
    if (hash_table[pos].value == 0) return;
    else {
      ans.push_back(hash_table[pos].value - 1);
      if (pmem_record_num_ != 0) {
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
    uint32_t pos = key & (hashSize - 1);
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
    const uint32_t hashSize = 1<<30;
    
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

class MyUInt64HashMap {
  public:
  MyUInt64HashMap() {
    hash_table = new uint32_t[hashSize];
    memset(hash_table, 0, sizeof(uint32_t) * hashSize);
  }

  ~MyUInt64HashMap() {
    delete hash_table;
  }
  //todo 缺少冲突管理
  uint32_t get(uint64_t key) {
    return hash_table[key & (hashSize - 1)]; //如果返回值是 0,表示是空的
  }
  //todo 缺少冲突管理
  void insert(uint64_t key, uint32_t value) {
    hash_table[key & (hashSize - 1)] = value + 1; //不可能出现0 
  }

  private:
  uint32_t *hash_table;
  const uint32_t hashSize = 1<<30;
};