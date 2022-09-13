#pragma once
#include <iostream>
#include <sys/mman.h>
#include <xmmintrin.h>
#include <unistd.h> 
#include <string.h>

struct alignas(64) MyStr256Head
{
  char data[256];
};

struct alignas(8) MyStrHead {
    uint32_t value = 0;
    std::vector<uint32_t> *ptr = nullptr;
};


class MyStringHashMap {
  public:
  MyStringHashMap() {
    hash_table = new MyStrHead[hashSize];
  }

  ~MyStringHashMap() {
    delete hash_table;
  }

 void get(uint64_t key, std::vector<uint32_t> &ans) {
    uint32_t pos = key & (hashSize - 1);
    if (hash_table[pos].value == 0) return;
    else {
      ans.push_back(hash_table[pos].value - 1);
      if (hash_table[pos].ptr != nullptr) {
        for (uint32_t i = 0; i < hash_table[pos].ptr->size(); i++) {
          ans.push_back(hash_table[pos].ptr->at(i));
        }
      }
    }
  }

  void insert(uint64_t key, uint32_t value) {
    uint32_t pos = key & (hashSize - 1);
    if (hash_table[pos].value == 0) {
      hash_table[pos].value = value + 1;
    } else {
      mtx.lock();
      if (hash_table[pos].ptr == nullptr) {
        hash_table[pos].ptr = new std::vector<uint32_t>();
      }
      hash_table[pos].ptr->push_back(value);
      mtx.unlock();
    }
  }

  void stat() {
    // std::cout << "recovery boom " << hash_boom << std::endl;
  }

  private:
    MyStrHead *hash_table;
    const uint32_t hashSize = 1<<30;
    std::mutex mtx;
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
  const uint32_t hashSize = 1<<28;
};