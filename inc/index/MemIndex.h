#pragma once
#include <cstdio>
#include <vector>

#include "../tools/HashFunc/xxhash.h"
#include "../tools/HashMap/EMHash/emhash7_int64_to_int32.h"

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
      return rhs.hashCode;
    }
};

uint32_t thread_pos[50] = {0};
static emhash7::HashMap<uint64_t, uint32_t> pk[HASH_MAP_COUNT];
static emhash7::HashMap<UserId, uint32_t, UserIdHash> uk[HASH_MAP_COUNT];
static emhash7::HashMap<uint64_t, std::vector<uint32_t>> sk[HASH_MAP_COUNT];

static void insert(const char *tuple, size_t len, uint8_t tid) {
    uint32_t pos = thread_pos[tid] + PER_THREAD_MAX_WRITE * tid;
    pk[tid].insert(std::pair<uint64_t, uint32_t>(*(uint64_t *)tuple, pos));
    uk[tid].insert(std::pair<UserId, uint32_t>(UserId(tuple + 8), pos));

    auto it = sk[tid].find(*(uint64_t *)(tuple + 264));
    if (it != sk[tid].end()) {
        it -> second.push_back(pos);
    } else {
        sk[tid].insert(std::pair<uint64_t, std::vector<uint32_t>>(*(uint64_t *)(tuple + 264), {pos}));
    }
    thread_pos[tid]++;
} 

static std::vector<uint32_t> getPosFromKey(int32_t where_column, const void *column_key) {
  std::vector<uint32_t> result;
  if (where_column == Id) {
    for (int i = 0; i < HASH_MAP_COUNT; i++) {
      auto it = pk[i].find(*(uint64_t *)(column_key));
      if (it != pk[i].end()) {
        return {it -> second};
      } 
    }
  }
  if (where_column == Userid) {
    for (int i = 0; i < HASH_MAP_COUNT; i++) {
      auto it = uk[i].find(UserId((char *)column_key));
      if (it != uk[i].end()) {
        return {it -> second};
      } 
    }    
  }
  if (where_column == Salary) {
    for (int i = 0; i < HASH_MAP_COUNT; i++) {
      auto it = sk[i].find(*(uint64_t *)((char *)column_key));
      if (it != sk[i].end()) {
        for (int j = 0; j < it -> second.size(); j++) {
          result.push_back(it -> second[j]);
        }
      }
    }
  }
  return result;
}
