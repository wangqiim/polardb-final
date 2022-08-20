#include "DB.h"
#include <stdio.h>
#include <string>
#include <mutex>
#include <atomic>
#include <sys/time.h>
#include <sys/sysinfo.h>
#include <sys/mman.h>
#include <xmmintrin.h>
#include <unistd.h>         

#include <iostream>
#include <fstream>
#include <fcntl.h> // for open()

#include <unordered_map>
#include <vector>

#include "Config.h"
#include "NvmBuffer.h"
#include "./tools/EMHash/hash_table6_int64_to_int64.h"
#include "./tools/EMHash/hash_table7_int64_to_int32.h"
#include "./tools/EMHash/hash_table8_str_to_int.h"
#include "./tools/MyStrHashMap.h"

#define likely(x) __builtin_expect(!!(x), 1) //gcc内置函数, 帮助编译器分支优化
#define unlikely(x) __builtin_expect(!!(x), 0)

#define DEBUG 1

static inline void print_user(User user) {
    printf("id:%ld\nsalary:%ld\nname:%s\nuserid:",
                                                    user.id,
                                                    user.id,
                                                    user.name);
    for(int i = 0; i < 128; i++) {
        printf("%u,",user.user_id[i]);
    }
    printf("\n");
    // printf("index:%lu\n",user.index);
}

static inline void print_time(struct timeval t1, struct timeval t2) {
    double timeuse = (t2.tv_sec - t1.tv_sec) + (double)(t2.tv_usec - t1.tv_usec)/1000000.0;
    std::cout<<"time = "<< timeuse << "s" << std::endl;  //输出时间（单位：ｓ）
}


static Status Recovery();


//Debug 相关
static uint64_t write_count = 0;
static int read_count = 0;
static bool is_full_recovery;
static struct timeval t1,t2; 

static int read_id_userId_count = 0;
static int read_id_real_count = 0;
static int read_userId_name_count = 0;
static int read_userId_real_count = 0;
static int read_salay_id_count = 0;
static int read_salay_max_real_count = 0;
static int read_salay_sum = 0;


static User *table;
//索引相关
static std::unordered_map<uint64_t, User> id_to_user[HASH_MAP_COUNT];               
static emhash8::HashMap<std::string, uint64_t> userId_to_id[HASH_MAP_COUNT];
static std::unordered_multimap<uint64_t, uint64_t> salary_to_id[HASH_MAP_COUNT]; 

class MyStr{
public:
  const char* data;
  MyStr(const char *str) {
    data = str;
  }
};

class My128Str{
public:
  char data[128];
  My128Str(const char *str) {
    memcpy(data, str, 128);
  }
};


class UserId{
public:
  uint64_t hashCode;
	UserId(const char *str){
    hashCode = *(uint64_t *)str;
	}
	bool operator==(const UserId & p) const 
	{
    return p.hashCode == hashCode;
	}
};

struct UserIdHash
{
    size_t operator()(const UserId& rhs) const{
      return rhs.hashCode;
    }
};

struct UInt64Hash
{
  size_t operator()(const uint64_t& rhs) const{
    return rhs;
  }
};

static emhash7::HashMap<uint64_t, uint64_t> pk;
static emhash7::HashMap<UserId, uint64_t, UserIdHash> uk;
static emhash7::HashMap<uint64_t, std::vector<uint64_t>> sk;


// static emhash8::HashMap<uint64_t, My128Str> pk_uk;
// static emhash8::HashMap<uint64_t, My128Str, UInt64Hash> uk_name;
// static emhash5::HashMap<uint64_t, uint64_t> sk_pk;

static MyStringHashMap pk__uk;
static MyStringHashMap uk__name;
// static MyUInt64HashMap sk__pk;

static void initNvmDB(const char* host_info, const char* const* peer_host_info, size_t peer_host_info_num,
                const char* aep_dir, const char* disk_dir){
  //CPU 相关
  // 获取核数
  printf("System has %ld processor(s). \n", sysconf(_SC_NPROCESSORS_CONF));
  initNvmBuffer(aep_dir, disk_dir);

  //表
  table = new User[TABLE_SIZE];
  Recovery();

}

static std::atomic<int> getTid(1);

static bool is_need_write = false;

static Status Put(const char *tuple, size_t len){
  static thread_local int tid = getTid++;
  static thread_local int thread_write = 0;
  _mm_prefetch(tuple, _MM_HINT_T0);
  Write(tuple, 264UL, tid - 1);

  if (write_count < 1000) {
    User *user = (User *)tuple;
    id_to_user[tid].insert(std::pair<uint64_t, User>(user -> id, *(user)));
    salary_to_id[tid].insert(std::pair<uint64_t, uint64_t>(*(uint64_t *)(tuple + 264), user -> id));
    userId_to_id[tid].insert(std::pair<std::string, uint64_t>(std::string(user -> user_id, 128), user -> id));  
    write_count++;
  }
  thread_write++;
  #if DEBUG
  if(thread_write % 1000000 == 0) {
    gettimeofday(&t2,NULL);
    printf("tid = %d ", tid - 1);
    print_time(t1, t2);
  } 
  #endif
  return 0;
}

static Status Get(int32_t select_column,
          int32_t where_column, const void *column_key, size_t column_key_len, void *res){

  if(is_full_recovery) {
    if (where_column == Id) {
      char *data = pk__uk.get(*(uint64_t *)column_key);
      memcpy(res, data, 128);
      return 1;
    }

    if (where_column == Userid) {
      char *data = uk__name.get(*(uint64_t *)column_key);
      memcpy(res, data, 128);
      return 1;
    }

    if (where_column ==  Salary) {
      // uint64_t result = sk__pk.get(*(uint64_t *)(column_key));
      memcpy(res, column_key, 8);
      return 1;
    }
  }

  std::vector<uint64_t> id_array;
  int recovery_count = 0;

  switch (where_column) {
  case Id:{
      // read_id_count++;
      auto it = pk.find(*(int64_t *)((char *)column_key));
      if (it != pk.end()) {
        id_array.push_back(it -> second);
        recovery_count = 1;
        break;
      } else if (is_full_recovery) break;

      for(int i = 0; i < HASH_MAP_COUNT; i++) {
          auto it = id_to_user[i].find(*(int64_t *)((char *)column_key));
          if (it != id_to_user[i].end()) {
              id_array.push_back(it -> first);
              // read_id_real_count++;
              break;
          }
      }    
      break;
  }
  case Userid: {
    // read_userId_count++;
      auto it = uk.find(((char *)column_key));
      if (it != uk.end()) {
        id_array.push_back(it -> second);
        recovery_count = 1;
        break;
      } else if (is_full_recovery) break;
      
      for(int i = 0; i < HASH_MAP_COUNT; i++) {
          auto it = userId_to_id[i].find(std::string((char *)column_key, 128));
          if (it != userId_to_id[i].end()) {
              id_array.push_back(it -> second);
              break;
          }
      }   
      break;
  }
  case Salary: {
    // read_salay_count++;
      auto it = sk.find(*(int64_t *)((char *)column_key));
      if (it != sk.end()) {
        id_array = it -> second;
        recovery_count = id_array.size();
        if(is_full_recovery) break;
      } else if (is_full_recovery) break;

      for (int i = 0; i < HASH_MAP_COUNT; i++) {
          	auto its = salary_to_id[i].equal_range(*(int64_t *)((char *)column_key));
            for (auto it = its.first; it != its.second; ++it) {
              id_array.push_back(it->second);
            }
      }
      // if(id_array.size() > 0) read_salay_real_count++;
      // read_salay_sum+=id_array.size();
      break;
  }
  default:
      break;
  }

  if(id_array.size() > 0)
  {
      switch(select_column) {
          case Id: 
              for (uint32_t i = 0; i < recovery_count; i++) {
                User user = table[id_array[i]];
                memcpy(res, &user.id, 8); 
                res = (char *)res + 8; 
              } 

              for(uint32_t k = recovery_count; k < id_array.size(); k++) {
                 for(int i = 0; i < HASH_MAP_COUNT; i++) {
                  auto it = id_to_user[i].find(id_array[k]);
                  if(it != id_to_user->end()) {
                    memcpy(res, &it->second, 8); 
                    res = (char *)res + 8; 
                    break;
                  }
                }               
              }
              break;
          case Userid: 
              for (uint32_t i = 0; i < recovery_count; i++) {
                User user = table[id_array[i]];
                memcpy(res, &user.user_id, 128); 
                res = (char *)res + 128; 
              } 

              for(uint32_t k = recovery_count; k < id_array.size(); k++) {
                 for(int i = 0; i < HASH_MAP_COUNT; i++) {
                  auto it = id_to_user[i].find(id_array[k]);
                  if(it != id_to_user->end()) {
                    memcpy(res, &it->second.user_id, 128); 
                    res = (char *)res + 128; 
                    break;
                  }
                }               
              }

              break;
          case Name:
              for (uint32_t i = 0; i < recovery_count; i++) {
                User user = table[id_array[i]];
                memcpy(res, &user.name, 128); 
                res = (char *)res + 128; 
              } 

              for(uint32_t k = recovery_count; k < id_array.size(); k++) {
                 for(int i = 0; i < HASH_MAP_COUNT; i++) {
                  auto it = id_to_user[i].find(id_array[k]);
                  if(it != id_to_user->end()) {
                    memcpy(res, &it->second.name, 128); 
                    res = (char *)res + 128; 
                    break;
                  }
                }               
              }
              break;
          case Salary:
              for (uint32_t i = 0; i < recovery_count; i++) {
                User user = table[id_array[i]];
                memcpy(res, &user.id, 8); 
                res = (char *)res + 8; 
              } 

              for(uint32_t k = recovery_count; k < id_array.size(); k++) {
                 for(int i = 0; i < HASH_MAP_COUNT; i++) {
                  auto it = id_to_user[i].find(id_array[k]);
                  if(it != id_to_user->end()) {
                    memcpy(res, &it->second.id, 8); 
                    res = (char *)res + 8; 
                  }
                }               
              }
              break;
          default: break; // wrong
      }
  }

  #if DEBUG
  read_count++;
  if(read_count % 1000000 == 0) {
    std::cout << "read count " << read_count << std::endl;
    gettimeofday(&t2,NULL);
    print_time(t1, t2);
  } 
  #endif
  return id_array.size();
}

static Status WriteMemory(const char *tuple, size_t len, uint64_t recovery_count) {
  if(recovery_count < 100000) {
    memcpy(table + recovery_count, tuple, 264UL);
    User *user = (User *)table + recovery_count;
    pk.insert(std::pair<uint64_t, uint64_t>(user -> id, recovery_count));
    uk.insert(std::pair<UserId, uint64_t>(UserId(user -> user_id), recovery_count));

    auto it = sk.find(user -> id);
    if (it != sk.end()) {
        it -> second.push_back(recovery_count);
    } else {
        sk.insert(std::pair<uint64_t, std::vector<uint64_t>>(user -> id, {recovery_count}));
    }
  }
  User *user = (User *)tuple;
  // pk_uk.insert(std::pair<uint64_t, My128Str>(user -> id, My128Str(tuple + 8UL)));
  // uk_name.insert(std::pair<uint64_t, My128Str>(*(uint64_t *)(tuple + 8UL), My128Str(tuple + 136UL)));
  // sk_pk.insert(std::pair<uint64_t, uint64_t>(user -> salary, user -> id));

  pk__uk.insert(user -> id, tuple + 8UL);
  uk__name.insert(*(uint64_t *)(tuple + 8UL), tuple + 136UL);
  // sk__pk.insert(user -> salary, user -> id);

  if(recovery_count % 1000000 == 0) {
    printf("recovery write count %d\n",recovery_count);
    gettimeofday(&t2,NULL);
    print_time(t1, t2);
  }
  return 0;
}

static Status Recovery() {

  gettimeofday(&t1,NULL);

  write_count = 0;
  read_count = 0;
  is_full_recovery = false;

  write_count = NvmBufferRecover(WriteMemory);
  printf("recovery %d tuples \n", write_count);
  if (write_count == 50000000) {
    is_full_recovery = true;
    // ReleaseMemory();
    // uk__name.stat();
  }
  gettimeofday(&t2,NULL);
  print_time(t1, t2);
  gettimeofday(&t1,NULL);
  write_count = 0;
  return 0;
}

static void deinitNvmDB() {
  std::cout << "Test end " << std::endl;
  gettimeofday(&t2,NULL);
  print_time(t1, t2);

  std::cout << "read_count:" << read_count << std::endl;
  std::cout << "read_id_userId_count:" << read_id_userId_count << std::endl;
  std::cout << "read_id_real_count:" << read_id_real_count << std::endl;
  std::cout << "read_userId_name_count:" << read_userId_name_count << std::endl;
  std::cout << "read_uerid_real_count:" << read_userId_real_count << std::endl;
  std::cout << "read_salary_id_count:" << read_salay_id_count << std::endl;
  std::cout << "read_salay_max_real_count:" << read_salay_max_real_count << std::endl;
  std::cout << "read_salary_sum:" << read_salay_sum << std::endl;
}