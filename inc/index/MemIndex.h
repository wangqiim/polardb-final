#pragma once
#include <cstdio>
#include <vector>
#include <unordered_map>
#include <mutex>
#include "../tools/HashFunc/xxhash.h"
#include "../tools/HashMap/EMHash/emhash5_int64_to_int64.h"
#include "../tools/HashMap/EMHash/emhash7_int64_to_int32.h"
#include "../tools/HashMap/EMHash/emhash8_str_to_int.h"
#include "../tools/HashMap/DenseMap/unordered_dense.h"
#include "../tools/MyHashMap/MyHashMap.h"
#include "../tools/MyHashMap/MyHashMapV2.h"
#include "../tools/MyHashMap/MyMultiMap.h"
#include "../tools/MyHashMap/MyMultiMapV2.h"
#include "../tools/MyStrHashMap.h"

static uint64_t local_max_pk = 0xFFFFFFFFFFFFFFFF, local_min_pk = 0; // 前提：性能阶段在写阶段完成之前，不修改{min, max}，没有远程读pk(否则性能会变差)。

static uint64_t blizardhashfn(const char *key) {
//    return ankerl::unordered_dense::detail::wyhash::hash(key, 128);
  // return XXH3_64bits(key, 128);
  return *(uint64_t *)(key);
}

// static uint32_t shardhashfn(uint64_t hash) {
//   uint32_t key = (hash >> 32) & 0xffffffff;
//   return key & (UK_HASH_MAP_SHARD - 1);
// }

// static uint32_t sk_shardhashfn(uint64_t hash) {
//   uint32_t key = (hash >> 32) & 0xffffffff;
//   return key & (SK_HASH_MAP_SHARD - 1);
// }

class UserId {
public:
  uint64_t hashCode;
  UserId() {}
	UserId(const char *str){
    hashCode = blizardhashfn(str);
	}
	UserId(const uint64_t hash){
    hashCode = hash;
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

//class Value {
//public:
//    uint32_t pos;
//    uint64_t id;
//    uint64_t salary;
//    Value(uint32_t pos_value, int64_t id_value, int64_t salary_value)
//        : pos(pos_value), id(id_value), salary(salary_value) {}
//};

//pthread_rwlock_t uk_rwlock[UK_HASH_MAP_SHARD];
//pthread_rwlock_t sk_rwlock[SK_HASH_MAP_SHARD];


static MyPKHashMap pk;
static MyUserIdHashMap uk(1<<30, "uk");
static MySalaryHashMap sk(1<<30, "sk");
// static MyMultiMap<uint64_t, uint32_t> sk[SK_HASH_MAP_SHARD];

// static emhash7::HashMap<uint64_t, uint32_t> pk[HASH_MAP_COUNT];
//static MyHashMapV2<UserId, uint32_t, UserIdHash> uk[UK_HASH_MAP_SHARD];
//static MyMultiMapV2<uint64_t, uint32_t> sk[SK_HASH_MAP_SHARD];
// static emhash7::HashMap<UserId, uint32_t, UserIdHash> uk[UK_HASH_MAP_SHARD];
// static emhash7::HashMap<uint64_t, std::vector<uint32_t>> sk[HASH_MAP_COUNT];

static void initIndex() {
  spdlog::info("Init Index Begin");

//  for (size_t i = 0; i < UK_HASH_MAP_SHARD; i++) {
//    uk[i].reserve(TOTAL_WRITE_NUM / UK_HASH_MAP_SHARD + 1);
//    pthread_rwlock_init(&uk_rwlock[i], NULL);
//  }

//  for (size_t i = 0; i < SK_HASH_MAP_SHARD; i++) {
//    sk[i].reserve(TOTAL_WRITE_NUM / SK_HASH_MAP_SHARD + 1);
//    pthread_rwlock_init(&sk_rwlock[i], NULL);
//  }

  spdlog::info("Init Index End");
}

// 该方法有两处调用
// 1. recovery时调用
// 2. write插入数据时调用
static void insert_idx(const char *tuple, __attribute__((unused)) size_t len, uint32_t pos) {
  uint64_t id = *(uint64_t *)tuple;
  uint64_t uk_hash = blizardhashfn(tuple + 8);
  uint64_t salary = *(uint64_t *)(tuple + 264);

  // 1. insert pk index
  if (!IsNormalPos(pos)) {
    pk.insert(id, pos);
  }
  // 2. insert uk index
  uk.insert(uk_hash, pos);
  // 3. insert sk index
  sk.insert(salary, pos);
}

static void getPosFromKey_Local(std::vector<uint32_t> &result, int32_t where_column, const void *column_key, uint32_t *need_remote_peers) {
  if (where_column == Id) {
    uint64_t key = *(uint64_t *)column_key;
     // performance test中,每个节点的数据是固定的连续两亿条,
     // 比如[0,2e8-1],[2e8, 4e8-1],[4e8, 6e8-1],[6e8, 8e8-1]
    if (key < local_min_pk || key > local_max_pk) return;
    
    if (id_range.first <= key && key < id_range.second) {
      // 普通数据
      if (is_sync_all || IsPosCommit(key - id_range.first)) {
        result.push_back(key - id_range.first);
      }
    } else {
      // 正确性阶段的随机数据
      uint32_t pos = pk.get(key);
      if (pos > 0) {
        result.push_back(pos - 1);
      }
    }
  } else if (where_column == Userid) {
    UserId uid((char *)column_key);
    uk.get(uid.hashCode, result, (const char *)column_key); // todo(wq): 网络请求只传递了hashcode,未传递实际数据
  } else if (where_column == Salary) {
    uint64_t salary = *(uint64_t *)((char *)column_key);
    sk.get(salary, result, need_remote_peers);
  }
  return;
}

static void getPosFromKey_Remote(std::vector<uint32_t> &result, int32_t where_column, const void *column_key, uint32_t *need_remote_peers) {
  if (where_column == Id) {
    uint64_t key = *(uint64_t *)column_key;
     // performance test中,每个节点的数据是固定的连续两亿条,
     // 比如[0,2e8-1],[2e8, 4e8-1],[4e8, 6e8-1],[6e8, 8e8-1]
    if (key < local_min_pk || key > local_max_pk) return;
    
    if (id_range.first <= key && key < id_range.second) {
      // 普通数据
      if (is_sync_all || IsPosCommit(key - id_range.first)) {
        result.push_back(key - id_range.first);
      }
    } else {
      // 正确性阶段的随机数据
      uint32_t pos = pk.get(key);
      if (pos > 0) {
        result.push_back(pos - 1);
      }
    }
  } else if (where_column == Userid) {
    uk.get(*(uint64_t *)column_key, result, (const char *)column_key); // todo(wq): 网络请求只传递了hashcode,未传递实际数据
  } else if (where_column == Salary) {
    uint64_t salary = *(uint64_t *)((char *)column_key);
    sk.get(salary, result, need_remote_peers);
  }
  return;
}

static void insertRemoteIdToSK(int peer_idx, uint32_t id, uint32_t salary) {
  sk.insert(salary, MySalaryHashMap::peeridx_offset(peer_idx, id));
}

static void getRemoteIdFromSK(uint64_t salary, char *res, bool &is_find) {
  is_find = sk.get_id(salary, (uint64_t *)res);
}

static void insertRemoteSalaryToPK(uint32_t id, uint64_t salary) {
  pk.insert_salary(id, salary);
}

static void getRemoteSalaryFromPK(uint64_t id, char *res, bool &is_find) {
  uint64_t salary = pk.get_salary(id);
  if (salary > 0) {
    is_find = true;
    salary -= 1;
    memcpy(res, &salary, 8);
  }
}