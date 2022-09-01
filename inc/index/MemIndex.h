#pragma once
#include <cstdio>
#include <vector>
#include <unordered_map>
#include <mutex>
#include "../tools/HashFunc/xxhash.h"
#include "../tools/HashMap/EMHash/emhash5_int64_to_int64.h"
#include "../tools/HashMap/EMHash/emhash7_int64_to_int32.h"
#include "../tools/HashMap/EMHash/emhash8_str_to_int.h"

static uint32_t crypttable[0x500] = {0};

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
};

struct UserIdHash {
    size_t operator()(const UserId& rhs) const{
      uint64_t res = rhs.hashCode1 << 32;
      res |= rhs.hashCode2;
      return res;
    }
};

class Str128 {
public:
  char data[128];
	Str128(const char *str){
    memcpy(data, str, 128);
	}
	bool operator==(const Str128 & p) const 
	{
    return memcmp(p.data, data, 128) == 0;
	}  
};

struct Str128Hash {
    size_t operator()(const Str128& rhs) const{
      return XXH3_64bits(rhs.data, 128);
    }
};

pthread_rwlock_t rwlock[50];
uint32_t thread_pos[50]; // 用来插索引时候作为value (第几个record)
// static emhash7::HashMap<uint64_t, uint32_t> pk[HASH_MAP_COUNT];
// static emhash5::HashMap<UserId, uint32_t, UserIdHash> uk[HASH_MAP_COUNT];
// static emhash7::HashMap<uint64_t, std::vector<uint32_t>> sk[HASH_MAP_COUNT];

static std::unordered_map<uint64_t, uint32_t> pk[HASH_MAP_COUNT];
static std::unordered_map<UserId, uint32_t, UserIdHash> uk[HASH_MAP_COUNT];
static std::unordered_multimap<uint64_t, uint32_t> sk[HASH_MAP_COUNT]; 

// static std::unordered_map<uint64_t, std::vector<uint32_t>> sk[HASH_MAP_COUNT];

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
    // pthread_rwlock_init(&rwlock[i], NULL);
    pk[i].reserve(4000000);
    uk[i].reserve(4000000);
    sk[i].reserve(4000000);
  }
  spdlog::info("Init Index End");
}

// 该方法有两处调用
// 1. recovery时调用
// 2. write插入数据时调用
static void insert(const char *tuple, size_t len, uint8_t tid) {
    // pthread_rwlock_wrlock(&rwlock[tid]);
    uint32_t pos = thread_pos[tid] + PER_THREAD_MAX_WRITE * tid;
    pk[tid].insert(std::pair<uint64_t, uint32_t>(*(uint64_t *)tuple, pos));
    uk[tid].insert(std::pair<UserId, uint32_t>(UserId(tuple + 8), pos));
    sk[tid].insert(std::pair<uint64_t, uint32_t>(*(uint64_t *)(tuple + 264), pos));
    // auto it = sk[tid].find(*(uint64_t *)(tuple + 264));
    // if (it != sk[tid].end()) {
    //     it -> second.push_back(pos);
    // } else {
    //     sk[tid].insert(std::pair<uint64_t, std::vector<uint32_t>>(*(uint64_t *)(tuple + 264), {pos}));
    // }
    thread_pos[tid]++;
    // pthread_rwlock_unlock(&rwlock[tid]);
} 

static std::vector<uint32_t> getPosFromKey(int32_t where_column, const void *column_key) {
  std::vector<uint32_t> result;
  if (where_column == Id) {
    for (int i = 0; i < HASH_MAP_COUNT; i++) {
      bool isFind = false;
      // pthread_rwlock_rdlock(&rwlock[i]);
      auto it = pk[i].find(*(uint64_t *)(column_key));
      if (it != pk[i].end()) {
        isFind = true;
      }
      if (isFind) result.push_back(it->second);
      // pthread_rwlock_unlock(&rwlock[i]);
    }
  }
  if (where_column == Userid) {
    for (int i = 0; i < HASH_MAP_COUNT; i++) {
      bool isFind = false;
      // pthread_rwlock_rdlock(&rwlock[i]);
      auto it = uk[i].find(UserId((char *)column_key));
      if (it != uk[i].end()) {
        isFind = true;
      } 
      if (isFind) result.push_back(it->second);
      // pthread_rwlock_unlock(&rwlock[i]);
    }    
  }
  if (where_column == Salary) {
    for (int i = 0; i < HASH_MAP_COUNT; i++) {
      // bool isFind = false;
      // pthread_rwlock_rdlock(&rwlock[i]);
      auto its = sk[i].equal_range(*(int64_t *)((char *)column_key));
      for (auto it = its.first; it != its.second; ++it) {
        result.push_back(it->second);
      }
      // auto it = sk[i].find(*(uint64_t *)((char *)column_key));
      // if (it != sk[i].end()) {
      //   isFind = true;
      // }
      // if (isFind) {
      //   for (int j = 0; j < it -> second.size(); j++) {
      //     result.push_back(it -> second[j]);
      //   }
      // }
      // pthread_rwlock_unlock(&rwlock[i]);
    }
  }

  return result;
}
