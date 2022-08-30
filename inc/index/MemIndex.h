#pragma once
#include <cstdio>
#include <vector>
#include <unordered_map>
#include <mutex>
#include "../tools/HashFunc/xxhash.h"
#include "../tools/HashMap/EMHash/emhash7_int64_to_int32.h"
#include "../tools/HashMap/EMHash/emhash8_str_to_int.h"
class UserId {
public:
  uint64_t hashCode;
	UserId(const char *str){
    hashCode = XXH3_64bits(str, 128);
	}
	bool operator==(const UserId & p) const 
	{
    return p.hashCode == hashCode;
	}
};

struct UserIdHash {
    size_t operator()(const UserId& rhs) const{
      return std::hash<uint64_t>{}(rhs.hashCode);
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
// static emhash7::HashMap<UserId, uint32_t, UserIdHash> uk[HASH_MAP_COUNT];
// static emhash7::HashMap<uint64_t, std::vector<uint32_t>> sk[HASH_MAP_COUNT];

static std::unordered_map<uint64_t, uint32_t> pk[HASH_MAP_COUNT];
static std::unordered_map<Str128, uint32_t, Str128Hash> uk[HASH_MAP_COUNT];
static std::unordered_map<uint64_t, std::vector<uint32_t>> sk[HASH_MAP_COUNT];

static void initIndex() {
  spdlog::info("Init Index Begin");
  memset(thread_pos, 0, sizeof(thread_pos));

  for (int i = 0; i < HASH_MAP_COUNT; i++) {
    pthread_rwlock_init(&rwlock[i], NULL);
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
    uk[tid].insert(std::pair<Str128, uint32_t>(Str128(tuple + 8), pos));

    auto it = sk[tid].find(*(uint64_t *)(tuple + 264));
    if (it != sk[tid].end()) {
        it -> second.push_back(pos);
    } else {
        sk[tid].insert(std::pair<uint64_t, std::vector<uint32_t>>(*(uint64_t *)(tuple + 264), {pos}));
    }
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
      auto it = uk[i].find(Str128((char *)column_key));
      if (it != uk[i].end()) {
        isFind = true;
      } 
      if (isFind) result.push_back(it->second);
      // pthread_rwlock_unlock(&rwlock[i]);
    }    
  }
  if (where_column == Salary) {
    for (int i = 0; i < HASH_MAP_COUNT; i++) {
      bool isFind = false;
      // pthread_rwlock_rdlock(&rwlock[i]);
      auto it = sk[i].find(*(uint64_t *)((char *)column_key));
      if (it != sk[i].end()) {
        isFind = true;
      }
      if (isFind) {
        for (int j = 0; j < it -> second.size(); j++) {
          result.push_back(it -> second[j]);
        }
      }
      // pthread_rwlock_unlock(&rwlock[i]);
    }
  }

  return result;
}
