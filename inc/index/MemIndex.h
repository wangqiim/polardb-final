#pragma once
#include <cstdio>
#include <vector>
#include <unordered_map>
#include <mutex>
#include "../tools/HashFunc/xxhash.h"

#include "oneapi/tbb/concurrent_unordered_map.h"

static uint32_t crypttable[0x500] = {0};

uint32_t hashfn(const char *key, int type) {
    uint8_t *k = (uint8_t *)key;
    uint32_t seed1 = 0x7fed7fed;
    uint32_t seed2 = 0xeeeeeeee;
    uint32_t ch;
    for(int i = 0; i < 128; i++) {
        ch = *k++;
        seed1 = crypttable[(type << 8) + ch] ^ (seed1 + seed2);
        seed2 = ch + seed1 + seed2 + (seed2 << 5) + 3;
    }
    return seed1;
}

// blizzard hash
class UserId {
public:
  uint32_t hashCode1;
  uint32_t hashCode2;
	UserId(const char *str){
    hashCode1  = hashfn(str, 1);
    hashCode2 =  hashfn(str, 2);
	}
	bool operator==(const UserId & p) const 
	{
    return p.hashCode1 == hashCode1 && p.hashCode2 == hashCode2;
	}
};

struct UserIdHash {
    size_t operator()(const UserId& rhs) const{
      uint64_t res = uint64_t(rhs.hashCode1) << 32;
      res |= rhs.hashCode2;
      return res;
    }
};

// 数据存储在pmem上, hashmap的key上存储一个指向pmem上UserId的指针
class PmemUserId {
public:
  const char *ptr_;
  PmemUserId(const char *ptr): ptr_(ptr) {}
	bool operator==(const PmemUserId& other) const 
	{
    return memcmp(ptr_, other.ptr_, 128) == 0;
	}
};

struct PmemUserIdHash {
    size_t operator()(const PmemUserId& pmem_user_id) const {
      return XXH3_64bits(pmem_user_id.ptr_, 128);
    }
};

uint32_t thread_pos[50]; // 用来插索引时候作为value (第几个record)

static tbb::concurrent_unordered_map<uint64_t, uint32_t> pk;
static tbb::concurrent_unordered_map<PmemUserId, uint32_t, PmemUserIdHash> uk;
static tbb::concurrent_unordered_multimap<uint64_t, uint32_t> sk;

static void initIndex() {
  spdlog::info("Init Index Begin");
  memset(thread_pos, 0, sizeof(thread_pos));

  uint32_t seed = 0x00100001, idx1, idx2, i;
  for (idx1 = 0; idx1 < 0x100; idx1++) {
      for (idx2 = idx1, i = 0; i < 5; i++, idx2 += 0x100) {
          uint32_t temp1, temp2;
          seed = (seed * 125 + 3) % 0x2AAAAB;
          temp1 = (seed & 0xFFFF) << 0x10;
          seed = (seed * 125 + 3) % 0x2AAAAB;
          temp2 = (seed & 0xFFFF);
          crypttable[idx2] = (temp1 | temp2);
      }
  }

  for (int i = 0; i < HASH_MAP_COUNT; i++) {
    pk.reserve(INDEX_CAPACITY);
    uk.reserve(INDEX_CAPACITY);
    sk.reserve(INDEX_CAPACITY);
  }
  spdlog::info("Init Index End");
}

// 该方法有两处调用
// 1. recovery时调用，注意：目前的实现中，此时tuple指向的地址在pmem上
// 2. write插入数据时调用，注意：目前的实现中，此时tuple指向的地址在pmem上
static void insert(const char *tuple, size_t len, uint8_t tid) {
    uint32_t pos = thread_pos[tid] + PER_THREAD_MAX_WRITE * tid;
    pk.insert(std::pair<uint64_t, uint32_t>(*(uint64_t *)tuple, pos));
    uk.insert(std::pair<PmemUserId, uint32_t>(PmemUserId(tuple + 8), pos));
    sk.insert(std::pair<uint64_t, uint32_t>(*(uint64_t *)(tuple + 264), pos));
    thread_pos[tid]++;
} 

static std::vector<uint32_t> getPosFromKey(int32_t where_column, const void *column_key) {
  std::vector<uint32_t> result;
  if (where_column == Id) {
    auto it = pk.find(*(uint64_t *)(column_key));
    if (it != pk.end()) {
      result.push_back(it->second);
    }
  }
  if (where_column == Userid) {
    auto it = uk.find(PmemUserId((char *)column_key));
    if (it != uk.end()) {
      result.push_back(it->second);
    } 
  }
  if (where_column == Salary) {
    auto its = sk.equal_range(*(int64_t *)((char *)column_key));
    for (auto it = its.first; it != its.second; ++it) {
      result.push_back(it->second);
    }
  }
  return result;
}
