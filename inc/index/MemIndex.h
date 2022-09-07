#pragma once
#include <cstdio>
#include <vector>
#include <unordered_map>
#include <mutex>
#include "../tools/HashFunc/xxhash.h"
#include "../tools/HashMap/EMHash/emhash5_int64_to_int64.h"
#include "../tools/HashMap/EMHash/emhash7_int64_to_int32.h"
#include "../tools/HashMap/EMHash/emhash8_str_to_int.h"
#include "../tools/DenseMap/unordered_dense.h"
#include "../tools/MyHashMap/MyMultiMap.h"
#include "../tools/MyStrHashMap.h"

static uint64_t local_max_pk = 0, local_min_pk = 0xFFFFFFFFFFFFFFFF;

static uint64_t blizardhashfn(const char *key) {
    return ankerl::unordered_dense::detail::wyhash::hash(key, 128);
}

static uint32_t shardhashfn(uint64_t hash) {
  uint32_t key = (hash >> 32) & 0xffffffff;
  return key & (UK_HASH_MAP_SHARD - 1);
}

static uint32_t sk_shardhashfn(uint64_t hash) {
  uint32_t key = (hash >> 32) & 0xffffffff;
  return key & (SK_HASH_MAP_SHARD - 1);
}

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

pthread_rwlock_t rwlock[3][MAX_HASH_MAP_SHARD];
uint32_t thread_pos[50]; // 用来插索引时候作为value (第几个record)

static MyMultiMap<uint64_t, uint32_t> sk2; // tudo(wq): 还未使用

static MyUInt64HashMap pk;

// static emhash7::HashMap<uint64_t, uint32_t> pk[HASH_MAP_COUNT];
static emhash7::HashMap<UserId, uint32_t, UserIdHash> uk[UK_HASH_MAP_SHARD];
// static emhash7::HashMap<uint64_t, std::vector<uint32_t>> sk[HASH_MAP_COUNT];

// static std::unordered_map<uint64_t, uint32_t> pk[HASH_MAP_COUNT];
// static std::unordered_map<UserId, uint32_t, UserIdHash> uk[HASH_MAP_COUNT];
static std::unordered_multimap<uint64_t, uint32_t> sk[SK_HASH_MAP_SHARD]; 

// static std::unordered_map<uint64_t, std::vector<uint32_t>> sk[HASH_MAP_COUNT];

static void initIndex() {
  spdlog::info("Init Index Begin");
  memset(thread_pos, 0, sizeof(thread_pos));

  for (size_t i = 0; i < UK_HASH_MAP_SHARD; i++) {
    uk[i].reserve(TOTAL_WRITE_NUM / UK_HASH_MAP_SHARD + 1);
    pthread_rwlock_init(&rwlock[1][i], NULL);
  }

  for (size_t i = 0; i < SK_HASH_MAP_SHARD; i++) {
    sk[i].reserve(TOTAL_WRITE_NUM / SK_HASH_MAP_SHARD + 1);
    pthread_rwlock_init(&rwlock[2][i], NULL);
  }

  spdlog::info("Init Index End");
}

// 该方法有两处调用
// 1. recovery时调用
// 2. write插入数据时调用
static void insert(const char *tuple, __attribute__((unused)) size_t len, uint8_t tid) {
    uint32_t pos = thread_pos[tid] + PER_THREAD_MAX_WRITE * tid;
    uint64_t id = *(uint64_t *)tuple;
    uint64_t uk_hash = blizardhashfn(tuple + 8);
    uint64_t salary = *(uint64_t *)(tuple + 264);

    uint32_t uk_shard = shardhashfn(uk_hash);
    uint64_t sk_shard = sk_shardhashfn(salary);

    //获取所有锁
    pthread_rwlock_wrlock(&rwlock[1][uk_shard]);
    pthread_rwlock_wrlock(&rwlock[2][sk_shard]);

    // 1. insert pk index
    pk.insert(id, pos);
    // uint64_t pk_shard = id % HASH_MAP_COUNT;
    // uint64_t pk_shard = tid; //不分片
    // pthread_rwlock_wrlock(&rwlock[0][pk_shard]);
    // pk[pk_shard].insert(std::pair<uint64_t, uint32_t>(id, pos));
    // pthread_rwlock_unlock(&rwlock[0][pk_shard]);
    // 2. insert uk index
    uk[uk_shard].insert(std::pair<UserId, uint32_t>(UserId(uk_hash), pos));
    // 3. insert sk index
    sk[sk_shard].insert(std::pair<uint64_t, uint32_t>(salary, pos));

    //释放所有锁
    pthread_rwlock_unlock(&rwlock[1][uk_shard]);
    pthread_rwlock_unlock(&rwlock[2][sk_shard]);

    if (id > local_max_pk) local_max_pk = id;
    if (id < local_min_pk) local_min_pk = id;
    thread_pos[tid]++;
} 

static std::vector<uint32_t> getPosFromKey(int32_t where_column, const void *column_key, bool is_local) {
  std::vector<uint32_t> result;
  if (where_column == Id) {
    uint64_t key = *(uint64_t *)column_key;
     // performance test中,每个节点的数据是固定的连续两亿条,
     // 比如[0,2e8-1],[2e8, 4e8-1],[4e8, 6e8-1],[6e8, 8e8-1]
    if (key < local_min_pk || key > local_max_pk) return result;
    uint32_t pos = pk.get(key);
    if (pos > 0) result.push_back(pos - 1);
    // uint64_t pk_shard = key % HASH_MAP_COUNT;
    // for (uint64_t pk_shard = 0; pk_shard < HASH_MAP_COUNT; pk_shard++) {
    //   pthread_rwlock_rdlock(&rwlock[0][pk_shard]);
    //   auto it = pk[pk_shard].find(key);
    //   if (it != pk[pk_shard].end()) {
    //     result.push_back(it->second);
    //     pthread_rwlock_unlock(&rwlock[0][pk_shard]);
    //     return result;
    //   }
    //   pthread_rwlock_unlock(&rwlock[0][pk_shard]);
    // }
  }
  if (where_column == Userid) {
    UserId uid;
    if(is_local) {
      uid = UserId((char *)column_key);
    } else { // 网络请求直接传递得到的是hashcode(user_id)而不是user_id[128]，降低网络带宽
      memcpy(&uid.hashCode, (char *)column_key, 8);
    }
    
    uint32_t uk_shard = shardhashfn(uid.hashCode);
    // pthread_rwlock_rdlock(&rwlock[1][uk_shard]);
    auto it = uk[uk_shard].find(uid);
    if (it != uk[uk_shard].end()) {
      result.push_back(it->second);
    } 
    // pthread_rwlock_unlock(&rwlock[1][uk_shard]);
  }
  if (where_column == Salary) {
    uint64_t salary = *(uint64_t *)((char *)column_key);
    // uint64_t sk_shard = salary % HASH_MAP_COUNT;
    // for (uint64_t sk_shard = 0; sk_shard < SK_HASH_MAP_SHARD; sk_shard++) {
    uint64_t sk_shard = sk_shardhashfn(salary);
    // pthread_rwlock_rdlock(&rwlock[2][sk_shard]);
    auto its = sk[sk_shard].equal_range(salary);
    for (auto it = its.first; it != its.second; ++it) {
      result.push_back(it->second);
    }
    // pthread_rwlock_unlock(&rwlock[2][sk_shard]);
    // }
  }

  return result;
}
