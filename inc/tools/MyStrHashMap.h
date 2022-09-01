#pragma once
#include <iostream>
#include <sys/mman.h>
#include <xmmintrin.h>
#include <unistd.h> 
#include <string.h>
#include "./xxhash.h"

struct alignas(64) MyStrHead
{
  char data[128];
};

struct alignas(64) MyStr256Head
{
  char data[256];
};


class MyStringHashMap {
  public:
  MyStringHashMap() {
    hash_table = new MyStrHead[hashSize];
  }

  ~MyStringHashMap() {
    delete hash_table;
  }

  char * get(uint32_t key) {
    return hash_table[key & (hashSize - 1)].data;
  }

  void insert(uint32_t key, const char * value) {
    memcpy(hash_table[key & (hashSize - 1)].data, value, 128);
  }

  void stat() {
    // std::cout << "recovery boom " << hash_boom << std::endl;
  }

  private:
  MyStrHead *hash_table;
  const uint32_t hashSize = 1<<26;
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
    hash_table = new uint64_t[hashSize];
    // memset(hash_table, 0, sizeof(MyStrHead) * hashSize);
  }

  ~MyUInt64HashMap() {
    delete hash_table;
  }

  uint64_t get(uint32_t key) {
    return hash_table[key & (hashSize)];
  }

  void insert(uint32_t key, uint64_t value) {
    hash_table[key & (hashSize)] = value;
  }

  void stat() {
    // std::cout << "recovery boom " << hash_boom << std::endl;
  }

  private:
  uint64_t *hash_table;
  const uint32_t hashSize = 1<<26 - 1;
};