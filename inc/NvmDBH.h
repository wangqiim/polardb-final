#include "DB.h"
#include <stdio.h>
#include <string>
#include <mutex>
#include <atomic>
#include <sys/time.h>
#include <sys/sysinfo.h>
#include <sys/mman.h>

#include <libpmemlog.h>
#include <xmmintrin.h>
#include <unistd.h>         

#include <iostream>
#include <fstream>
#include <fcntl.h> // for open()

#include <unordered_map>
#include <vector>

#include "Config.h"
#include "./tools/BlizzardHashMap.h"
#include "./tools/EMHash/hash_table7_int64_to_int32.h"
#include "./tools/EMHash/hash_table8_str_to_int.h"

#define DEBUG 1

static inline void print_user(User user) {
    printf("id:%ld\nsalary:%ld\nname:%s\nuserid:",
                                                    user.id,
                                                    user.salary,
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

static inline bool exists_test (const std::string& name) {
    std::ifstream f(name.c_str());
    return f.good();
}


static Status Recovery();


//Debug 相关
static int write_count = 0;
static int read_count = 0;
static bool is_full_recovery;
static struct timeval t1,t2; 

static int read_id_count = 0;
static int read_id_real_count = 0;
static int read_userId_count = 0;
static int read_userId_real_count = 0;
static int read_salay_count = 0;
static int read_salay_real_count = 0;
static int read_salay_sum = 0;

//PMEM相关
static char * __restrict__ pmem_address_[PMEM_FILE_COUNT];                  //PMEM地址起点
static uint64_t pos_[PMEM_FILE_COUNT];                      //第一条可用位置

//表相关
static User *table;

#define POOL_SIZE 15*1024*1024*1024  // 15G
PMEMlogpool *plp = nullptr;

//索引相关
static std::unordered_map<uint64_t, User> id_to_user[HASH_MAP_COUNT];               
static emhash8::HashMap<std::string, uint64_t> userId_to_id[HASH_MAP_COUNT];
static std::unordered_multimap<uint64_t, uint64_t> salary_to_id[HASH_MAP_COUNT]; 
static struct hashtable *userId_to_pos_; //暴雪hash算法实现的hashMap 用于 user_id >> tuple

int make_indexs(const void *buf, size_t len, void *arg)
{
    // 将该tuple写入到内存索引中
    return 0;
}

static void initNvmDB(const char* host_info, const char* const* peer_host_info, size_t peer_host_info_num,
                const char* aep_dir, const char* disk_dir){
  //CPU 相关
  // 获取核数
  printf("System has %ld processor(s). \n", sysconf(_SC_NPROCESSORS_CONF));

  std::string path = std::string(aep_dir) + "pmemlog.file";
  //初始化PMEM
  std::string base_path = std::string(aep_dir);
  if(base_path[base_path.length() - 1] != '/') {
      base_path = base_path + "/";
  }

  std::string base_disk = std::string(disk_dir);
  if(base_disk[base_disk.length() - 1] != '/') {
      base_disk = base_disk + "/";
  }  

  plp = pmemlog_create(path.c_str(), POOL_SIZE, 0666);
  bool is_first_init = true;

  if (plp == NULL)
  {
      plp = pmemlog_open(path);
      is_first_init = false;
  }

  if (plp == NULL)
  {
      // 报错
      exit(1);
  }

  if(!is_first_init)
  {
      /*
        * 做缓冲区预热
        * 将AEP上的数据读取出来，加载到几个索引结构中（hashmap）
        */
      pmemlog_walk(plp, sizeof(User), make_indexs, NULL);
  }

  std::string pmem_path[PMEM_FILE_COUNT];
  for (int i = 0; i < PMEM_FILE_COUNT; i++) {
    unsigned long mmap_size = PMEM_SIZE;
    if(i < 30) {
      // pmem_path[i] = base_path + "pm"+ std::to_string(i) + ".pool";
    } else {
      pmem_path[i] = base_disk + "disk"+ std::to_string(i) + ".pool";
      printf("访问的完整路径:%s\n",pmem_path[i].c_str());

      int fd;
      if(exists_test(pmem_path[i])) {
        fd = open(pmem_path[i].c_str(), O_RDWR);
        lseek(fd,mmap_size - 1,SEEK_SET);
      } else {
        fd = open(pmem_path[i].c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
        lseek(fd, mmap_size - 1, SEEK_SET);
        write(fd, "0", 1);
      }
      if (fd == -1) {
        printf("Open file %s failed.\n", pmem_path[i].c_str());
        exit(-1);
      }

      // solve the bus error problem:
      // we should allocate space for the file first.
      pmem_address_[i] = (char *)mmap(0, mmap_size, PROT_WRITE | PROT_READ, MAP_SHARED, fd, 0);
      if (pmem_address_[i] == MAP_FAILED) {
        printf("Map failed.\n");
        exit(-1);
      } else {
        printf("Map success.\n");
      }

      close(fd);
      pos_[i] = 0;    
    }
  }

  //索引
  hashtable_global_init(NULL, NULL);
  userId_to_pos_ = hashtable_create(2<<25);  

  Recovery();

}

static std::atomic<int> getTid(1);


static Status Put(const char *tuple, size_t len){
  static thread_local int tid = getTid++;
  static thread_local uint32_t thread_write_pos = pos_[tid - 1];
  uint32_t current_index = tid * 1500000 + thread_write_pos;

  uint64_t tmp_pos = thread_write_pos * 272UL;

  _mm_prefetch(tuple, _MM_HINT_T0);
  User *user = (User *)tuple;
  // printf("need write %u pos %lu \n", tid, tmp_pos);
  if(tid > 30) {
    memcpy(pmem_address_[tid - 1] + tmp_pos, tuple, 272UL);
  } else {

    // 调用pmemlog_append写入数据
    if (pmemlog_append(plp, tuple, 272UL) < 0) {
        perror("pmemlog_append");
        exit(1);
    }
  }

  if (write_count < 150000) {
  id_to_user[tid].insert(std::pair<uint64_t, User>(user -> id, *(user)));
  // auto it = salary_to_id[tid].find(user -> salary);
  // if (it != salary_to_id[tid].end()) {
  //     salary_to_id[tid][it -> first].push_back(user -> id);
  // } else {
  //     salary_to_id[tid].insert(std::pair<uint64_t, std::vector<uint64_t>>(user -> salary, {*(uint64_t *)tuple}));
  // }
  salary_to_id[tid].insert(std::pair<uint64_t, uint64_t>(user -> salary, user -> id));
  userId_to_id[tid].insert(std::pair<std::string, uint64_t>(std::string(user -> user_id, 128), user -> id));  
  }
  
  write_count++;
  thread_write_pos++;
  #if DEBUG
  if(write_count % 1000000 == 0) {
    std::cout << "write count " << write_count << std::endl;
    gettimeofday(&t2,NULL);
    print_time(t1, t2);
  } 
  #endif
  return 0;
}

static Status Get(int32_t select_column,
          int32_t where_column, const void *column_key, size_t column_key_len, void *res){

  std::vector<uint64_t> id_array;
  std::vector<User> user_array;
  bool is_salary = false;
  switch (where_column) {
  case Id:{
      read_id_count++;
      for(int i = 0; i < HASH_MAP_COUNT; i++) {
          auto it = id_to_user[i].find(*(int64_t *)((char *)column_key));
          if (it != id_to_user[i].end()) {
              id_array.push_back(it -> first);
              read_id_real_count++;
              break;
          }
      }        
      break;
  }
  case Userid: {
    read_userId_count++;
      uint64_t id = hashtable_get(userId_to_pos_, (char *)column_key);
      if(id != 0) {
          id_array.push_back(id);
          read_userId_real_count++;
      } else {
        for(int i = 0; i < HASH_MAP_COUNT; i++) {
            auto it = userId_to_id[i].find(std::string((char *)column_key, 128));
            if (it != userId_to_id[i].end()) {
                id_array.push_back(it -> second);
                break;
            }
        }
      }       
      break;
  }
  case Salary: {
    read_salay_count++;
      for (int i = 0; i < HASH_MAP_COUNT; i++) {
          // auto it = salary_to_id[i].find(*(int64_t *)((char *)column_key));
          // if (it != salary_to_id[i].end()) {
          //     for(int k = 0; k < it->second.size(); k++) {
          //       id_array.push_back(it->second[k]);
          //     }
          //     if(i == 0) recovery_index = id_array.size();
          //     if(is_full_recovery) {
          //       // is_salary = true;
          //       break;
          //     }
          // }
          	auto its = salary_to_id[i].equal_range(*(int64_t *)((char *)column_key));
            for (auto it = its.first; it != its.second; ++it) {
              id_array.push_back(it->second);
            }
            if (is_full_recovery) break;
      }
      if(id_array.size() > 0) read_salay_real_count++;
      read_salay_sum+=id_array.size();
      break;
  }
  default:
      break;
  }

  if(id_array.size() > 0)
  {
      switch(select_column) {
          case Id: 
              // if(is_salary) {
              //   u_int64_t len = 8 * salary_cluster_table[pos_array[0]].size;
              //   u_int64_t pos = 0;
              //   memcpy(res, salary_cluster_table[pos_array[0]].data + pos, len);
              //   return salary_cluster_table[pos_array[0]].size;
              // }
              for(int k = 0; k < id_array.size(); k++) {
                 for(int i = 0; i < HASH_MAP_COUNT; i++) {
                  auto it = id_to_user[i].find(id_array[k]);
                  if(it != id_to_user->end()) {
                    memcpy(res, &it->second, 8); 
                    res = (char *)res + 8; 
                    break;
                  }
                }               
              }

              // for(unsigned int k = 0; k < recovery_index; k++) {
              //   for(int i = 0; i < HASH_MAP_COUNT; i++) {
              //     User user = id_to_user->at(id_array[k]);
              //   }
              //     memcpy(res, &table[id_array[k]].id, 8); 
              //     res = (char *)res + 8; 
              // }
              // for(unsigned int k = recovery_index; k < pos_array.size(); k++) {
              //     int tid = pos_array[k] / 1500000;
              //     uint64_t pos = (pos_array[k] % 1500000) * 272UL;
              //     memcpy(res, pmem_address_[tid - 1] + pos, 8); 
              //     res = (char *)res + 8; 
              // }
              break;
          case Userid: 
              // if(is_salary) {
              //   u_int64_t len = 128 * salary_cluster_table[pos_array[0]].size;
              //   u_int64_t pos = 8 * salary_cluster_table[pos_array[0]].size;
              //   memcpy(res, salary_cluster_table[pos_array[0]].data + pos, len);                
              //   return salary_cluster_table[pos_array[0]].size;
              // }
              // for(unsigned int k = 0; k < recovery_index; k++) {
              //     memcpy(res, &table[pos_array[k]].user_id, 128); 
              //     res = (char *)res + 128; 
              // }
              // for(unsigned int k = recovery_index; k < pos_array.size(); k++) {
              //     int tid = pos_array[k] / 1500000;
              //     uint64_t pos = (pos_array[k] % 1500000) * 272UL;
              //     memcpy(res, pmem_address_[tid - 1] + pos + 8UL, 128UL); 
              //     res = (char *)res + 128; 
              // }
              for(int k = 0; k < id_array.size(); k++) {
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
              // if(is_salary) {
              //   u_int64_t len = 128 * salary_cluster_table[pos_array[0]].size;
              //   u_int64_t pos = 136 * salary_cluster_table[pos_array[0]].size;
              //   memcpy(res, salary_cluster_table[pos_array[0]].data + pos, len);                  
              //   return salary_cluster_table[pos_array[0]].size;
              // }
              // for(unsigned int k = 0; k < recovery_index; k++) {
              //     memcpy(res, &table[pos_array[k]].name, 128); 
              //     res = (char *)res + 128; 
              // }
              // for(unsigned int k = recovery_index; k < pos_array.size(); k++) {
              //     int tid = pos_array[k] / 1500000;
              //     uint64_t pos = (pos_array[k] % 1500000) * 272UL;
              //     memcpy(res, pmem_address_[tid - 1] + pos + 136UL, 128UL); 
              //     res = (char *)res + 128; 
              // }
              for(int k = 0; k < id_array.size(); k++) {
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
              // if(is_salary) {
              //   u_int64_t len = 128 * salary_cluster_table[pos_array[0]].size;
              //   u_int64_t pos = 264 * salary_cluster_table[pos_array[0]].size;
              //   memcpy(res, salary_cluster_table[pos_array[0]].data + pos, len);                  
              //   return salary_cluster_table[pos_array[0]].size;
              // }
              // for(unsigned int k = 0; k < recovery_index; k++) {
              //     memcpy(res, &table[pos_array[k]].salary, 8); 
              //     res = (char *)res + 8; 
              // }
              // for(unsigned int k = recovery_index; k < pos_array.size(); k++) {
              //     int tid = pos_array[k] / 1500000;
              //     uint64_t pos = (pos_array[k] % 1500000) * 272UL;
              //     memcpy(res, pmem_address_[tid - 1] + pos + 264UL, 8UL); 
              //     res = (char *)res + 8; 
              // }
              for(int k = 0; k < id_array.size(); k++) {
                 for(int i = 0; i < HASH_MAP_COUNT; i++) {
                  auto it = id_to_user[i].find(id_array[k]);
                  if(it != id_to_user->end()) {
                    memcpy(res, &it->second.salary, 8); 
                    res = (char *)res + 8; 
                  }
                  if(is_full_recovery) break;
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

// static Status initSalaryClusterTable() {
//   printf("begin init salary cluster table\n");
//   int c_count = 0;
//   for(auto it = salary_to_id[0].begin(); it != salary_to_id[0].end();  it++)
//   {
//       std::vector<uint32_t> pos_array = it -> second;
//       NewUser user;
//       user.size = pos_array.size();
//       user.data = new char[272 * user.size];
//       for(uint32_t i = 0; i < pos_array.size(); i++) {
//         memcpy(user.data + i * 8UL, &table[pos_array[i]].id, 8UL);
//         memcpy(user.data + pos_array.size() * 8UL + i * 128UL, &table[pos_array[i]].user_id, 128UL);
//         memcpy(user.data + pos_array.size() * 136UL + i * 128UL, &table[pos_array[i]].name, 128UL);
//         memcpy(user.data + pos_array.size() * 264UL + i * 8UL, &table[pos_array[i]].salary, 8UL);
//       }
//       salary_cluster_table.push_back(user);
//       it -> second.clear();
//       it -> second.push_back(c_count++);
//   }
//   std::cout << c_count << std::endl;
//   gettimeofday(&t2,NULL);
//   print_time(t1, t2);
// }


static Status WriteMemory(const char *tuple, size_t len, uint32_t current_index) {
  User *user = (User *)tuple;
  // memcpy(&table[current_index], tuple, len);
  id_to_user[0].insert(std::pair<uint64_t, User>(user -> id, *user));
  // salary_to_pos_[0].insert(std::pair<int64_t, uint32_t>(table[current_index].salary, current_index));
  // auto it = salary_to_id[0].find(user->salary);
  // if (it != salary_to_id[0].end()) {
  //     salary_to_id[0][it -> first].push_back(user.id);
  // } else {
  //     salary_to_id[0].insert(std::pair<int64_t, std::vector<uint64_t>>(user.salary, {user.id}));
  // }
  salary_to_id[0].insert(std::pair<uint64_t, uint64_t>(user -> salary, user -> id));
  hashtable_put(userId_to_pos_, user -> user_id, user -> id);
  return 0;
}

static Status Recovery() {

  gettimeofday(&t1,NULL);

  write_count = 0;
  read_count = 0;
  is_full_recovery = false;

  User *user;
  uint64_t recovery_pos = 0UL;


  for(int i = 30; i < PMEM_FILE_COUNT; i++) {
    user = (User *)(pmem_address_[i] + recovery_pos);
  //   print_user(user);
    while(user -> id != 0 || user -> salary !=0) {
        recovery_pos += 272UL;
        WriteMemory((char *)(user), 272UL, write_count);
        user = (User *)(pmem_address_[i] + recovery_pos);
        write_count++;
        #if DEBUG
        if(write_count % 1000000 == 0) {
          std::cout << "recovery count " << write_count << std::endl;
          gettimeofday(&t2,NULL);
          print_time(t1, t2);
        }
        #endif
    }
    pos_[i] = recovery_pos / 272UL;
    recovery_pos = 0UL;
  }
  if(write_count - 50000000 == 0) is_full_recovery = true;
  printf("recovery %d tuples \n", write_count);
  write_count = 0;
  gettimeofday(&t2,NULL);
  print_time(t1, t2);

  // if(is_full_recovery) initSalaryClusterTable();
  return 0;
}

static void deinitNvmDB() {
  std::cout << "read_count:" << read_count << std::endl;
  std::cout << "read_id_count:" << read_id_count << std::endl;
  std::cout << "read_id_real_count:" << read_id_real_count << std::endl;
  std::cout << "read_uerid_count:" << read_userId_count << std::endl;
  std::cout << "read_uerid_real_count:" << read_userId_real_count << std::endl;
  std::cout << "read_salary_count:" << read_salay_count << std::endl;
  std::cout << "read_salary_real_count:" << read_salay_real_count << std::endl;
  std::cout << "read_salary_sum:" << read_salay_sum << std::endl;
}