#ifndef HRH_NVMDB_H
#define HRH_NVMDB_H

#include "../inc/DB.h"
#include "../inc/Config.h"
#include "../inc/tools/BlizzardHashMap.h"
#include "../inc/tools/EMHash/hash_table7_int64_to_int32.h"
#include "../inc/tools/EMHash/hash_table8_str_to_int.h"
#include <unordered_map>
#include <vector>
#include <array>
#include <atomic>


class NvmDB : DB {
public:
  NvmDB();

  NvmDB(const char* host_info, const char* const* peer_host_info, size_t peer_host_info_num,
                  const char* aep_dir, const char* disk_dir);

  Status Put(const char *tuple, size_t len);

  Status Get(int32_t select_column,
            int32_t where_column, const void *column_key, size_t column_key_len, void *res);

  Status Recovery();

  ~NvmDB();
private:
  //CPU相关
  int CPU_NUM;

  //Debug 相关
  int write_count = 0;
  int read_count = 0;
  struct timeval t1,t2; 

  //PMEM相关
  char *pmem_address_[PMEM_FILE_COUNT];                  //PMEM地址起点
  size_t mapped_len_[PMEM_FILE_COUNT];                   //PMEM容量
  int32_t is_pmem_[PMEM_FILE_COUNT];                     //标记
  uint64_t pos_[PMEM_FILE_COUNT];                      //第一条可用位置

  //表相关
  User *table;

  //索引相关
  emhash7::HashMap<uint64_t, uint32_t> id_to_pos_[HASH_MAP_COUNT];               //id索引到table中tuple偏移
  std::unordered_multimap<uint64_t, uint32_t> salary_to_pos_[HASH_MAP_COUNT];  //salary索引到table中多条偏移
  emhash8::HashMap<std::string, uint32_t> userId_to_pos_up_[HASH_MAP_COUNT];
  struct hashtable *userId_to_pos_; //暴雪hash算法实现的hashMap 用于 user_id >> tuple

  Status WriteMemory(const char *tuple, size_t len, uint32_t current_index);
};

#endif