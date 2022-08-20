#include "../inc/NvmDB.h"
#include <libpmem.h>
#include <stdio.h>
#include <string>
#include <string.h>
#include <mutex>
#include <sys/time.h>
#include <sys/sysinfo.h>
#include <sys/types.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <xmmintrin.h>
#include <unistd.h>         
#include <pthread.h>
#include <sched.h>
#include <ctype.h>
#include <iostream>
#include <fstream>
#include <fcntl.h> // for open()
#include <sys/mman.h>

#include "../inc/tools/SpinMutex.h"

#define DEBUG 1
// tshm::Hashmap<std::string, int64_t, ll::AddOnlyLockFreeLL> userId_to_pos_link(20000000);

void print_user(User user) {
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

void print_time(struct timeval t1, struct timeval t2) {
    double timeuse = (t2.tv_sec - t1.tv_sec) + (double)(t2.tv_usec - t1.tv_usec)/1000000.0;
    std::cout<<"time = "<< timeuse << "s" << std::endl;  //输出时间（单位：ｓ）
}

inline bool exists_test (const std::string& name) {
    std::ifstream f(name.c_str());
    return f.good();
}


NvmDB::NvmDB(){}

NvmDB::NvmDB(const char* host_info, const char* const* peer_host_info, size_t peer_host_info_num,
                const char* aep_dir, const char* disk_dir){
  //CPU 相关
  // 获取核数
  CPU_NUM = sysconf(_SC_NPROCESSORS_CONF);
  printf("System has %i processor(s). \n", CPU_NUM);

  //初始化PMEM
  std::string base_path = std::string(aep_dir);
  if(base_path[base_path.length() - 1] != '/') {
      base_path = base_path + "/";
  }

  std::string base_disk = std::string(disk_dir);
  if(base_disk[base_disk.length() - 1] != '/') {
      base_disk = base_disk + "/";
  }  

  std::string pmem_path[PMEM_FILE_COUNT];
  for (int i = 0; i < PMEM_FILE_COUNT; i++) {
    unsigned long mmap_size = PMEM_SIZE;
    if(i < 6) {
      pmem_path[i] = base_path + "pm"+ std::to_string(i) + ".pool";
      mmap_size *= 2;
    } else {
      pmem_path[i] = base_disk + "disk"+ std::to_string(i) + ".pool";
    }
    printf("访问的完整路径:%s\n",pmem_path[i].c_str());

//   /* create a pmem file and memory map it */
//   if ( (pmem_address_ = (char *)pmem_map_file(all_path.c_str(), PMEM_SIZE, PMEM_FILE_CREATE,
//     0666, &mapped_len_, &is_pmem_)) == NULL ) {
//     perror("pmem_map_file");
//   }   

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

  table = new User[TABLE_SIZE];
  // table = (User *)malloc(sizeof(User) * TABLE_SIZE * 1.5);

  //索引
  hashtable_global_init(NULL, NULL);
  userId_to_pos_ = hashtable_create(2<<26);  

  Recovery();

}

DB::~DB(){}
NvmDB::~NvmDB(){
  
}

spin_mutex pmem_mtx;                  //用自旋锁访问PMEN性能会比用互斥锁高，减小线程切换的开销
std::mutex pmem_mutex[PMEM_FILE_COUNT];
static std::atomic<int> getNum(1);


Status NvmDB::Put(const char *tuple, size_t len){
  static thread_local int tid = getNum++;
  static thread_local uint32_t thread_write_pos = pos_[tid];
  uint32_t current_index = tid * 1500000 + thread_write_pos;
  // memcpy(&table[current_index], tuple, len);

  uint64_t tmp_pos = thread_write_pos * 272UL;
  // memcpy(&table[current_index].index, &tmp_pos, 8UL);
  _mm_prefetch(tuple, _MM_HINT_T0);
  _mm_prefetch(tuple + 64, _MM_HINT_T0);
  _mm_prefetch(tuple + 128, _MM_HINT_T0);
  _mm_prefetch(tuple + 192, _MM_HINT_T0);
  // printf("need write %u pos %lu \n", tid, tmp_pos);
  memcpy(pmem_address_[tid] + tmp_pos, tuple, 272UL);

  if(write_count < 10000) {
    id_to_pos_[tid].insert(std::pair<uint64_t, uint32_t>(*(int64_t *)((char *)tuple), current_index));
    salary_to_pos_[tid].insert(std::pair<int64_t, uint32_t>(*(int64_t *)((char *)tuple + 264), current_index));
    userId_to_pos_up_[tid].insert(std::pair<std::string, uint32_t>(std::string(tuple + 8, 128), current_index));  
  }
  // std::cout << *(int64_t *)((char *)tuple) << "\n" << *(int64_t *)((char *)tuple + 264) << "\n" << std::string(tuple + 8, 128) << "\n" << current_index << std::endl;

  // char res[2000];
  // std::cout << Get(Salary, Id, ((char *)tuple), 8, res) << std::endl;
  // // std::cout << Get(Name, Salary, ((char *)tuple + 264), 8, res) << std::endl;
  // exit(1);

  // if(write_count < 1000) {
  //   id_to_pos_[tid].insert(std::pair<uint64_t, uint32_t>(table[current_index].id, thread_write_pos));
  //   salary_to_pos_[tid].insert(std::pair<int64_t, uint32_t>(table[current_index].salary, thread_write_pos));
  //   userId_to_pos_up_[tid].insert(std::pair<std::string, uint32_t>(std::string(table[current_index].user_id, 128), thread_write_pos));  
  // }
    
//   pmem_mutex[use_pmem_index].lock();
//   memcpy(&table[current_index].index, &pos_[use_pmem_index], 8UL);
// //   print_user(table[current_index]);
//   pmem_memcpy_nodrain(pmem_address_[use_pmem_index] + pos_[use_pmem_index], &table[current_index], 280UL);
//   pos_[use_pmem_index] = pos_[use_pmem_index] + 280UL;
//   pmem_mutex[use_pmem_index].unlock();

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

Status NvmDB::Get(int32_t select_column,
          int32_t where_column, const void *column_key, size_t column_key_len, void *res){

  std::vector<uint32_t> pos_array;
  uint32_t recovery_index = 0;
  switch (where_column) {
  case Id:{
      for(int i = 0; i < HASH_MAP_COUNT; i++) {
          auto it = id_to_pos_[i].find(*(int64_t *)((char *)column_key));
          if (it != id_to_pos_[i].end()) {
              pos_array.push_back(it -> second);
              if(i == 0) recovery_index = 1;
              // printf("find id in %u, index = %lu\n", i, it->second);
              break;
          }
      }        
      break;
  }
  case Userid: {
      uint32_t id = hashtable_get(userId_to_pos_, (char *)column_key);
      if(id != 0) {
          recovery_index = 1;
          pos_array.push_back(id);
      } else {
        for(int i = 1; i < HASH_MAP_COUNT; i++) {
            auto it = userId_to_pos_up_[i].find(std::string((char *)column_key, 128));
            if (it != userId_to_pos_up_[i].end()) {
                pos_array.push_back(it -> second);
                break;
            }
        }
      }       
      break;
  }
  case Salary: {
      for (int i = 0; i < HASH_MAP_COUNT; i++) {
        auto pr = salary_to_pos_[i].equal_range(*(int64_t *)((char *)column_key)); 
        while(pr.first != pr.second)
        {
            pos_array.push_back(pr.first->second);
            ++pr.first; // Increment begin iterator
        }
        if(i == 0 && pos_array.size() > 0) recovery_index = pos_array.size();
          // auto it = salary_to_pos_[i].find(*(int64_t *)((char *)column_key));
          // if (it != salary_to_pos_[i].end()) {
          //     pos_array = it -> second;
          // }
      }
      break;
  }
  default:
      break;
  }

  if(pos_array.size() > 0)
  {
      switch(select_column) {
          case Id: 
              for(unsigned int k = 0; k < recovery_index; k++) {
                  memcpy(res, &table[pos_array[k]].id, 8); 
                  res = (char *)res + 8; 
              }
              for(unsigned int k = recovery_index; k < pos_array.size(); k++) {
                  int tid = pos_array[k] / 1500000;
                  uint64_t pos = (pos_array[k] % 1500000) * 272UL;
                  memcpy(res, pmem_address_[tid] + pos, 8); 
                  res = (char *)res + 8; 
              }
              break;
          case Userid: 
              for(unsigned int k = 0; k < recovery_index; k++) {
                  memcpy(res, &table[pos_array[k]].user_id, 128); 
                  res = (char *)res + 128; 
              }
              for(unsigned int k = recovery_index; k < pos_array.size(); k++) {
                  int tid = pos_array[k] / 1500000;
                  uint64_t pos = (pos_array[k] % 1500000) * 272UL;
                  memcpy(res, pmem_address_[tid] + pos + 8, 128); 
                  res = (char *)res + 128; 
              }
              break;
          case Name: 
              for(unsigned int k = 0; k < recovery_index; k++) {
                  memcpy(res, &table[pos_array[k]].name, 128); 
                  res = (char *)res + 128; 
              }
              for(unsigned int k = recovery_index; k < pos_array.size(); k++) {
                  int tid = pos_array[k] / 1500000;
                  uint64_t pos = (pos_array[k] % 1500000) * 272UL;
                  memcpy(res, pmem_address_[tid] + pos + 136, 128); 
                  res = (char *)res + 128; 
              }
              break;
          case Salary: 
              for(unsigned int k = 0; k < recovery_index; k++) {
                  memcpy(res, &table[pos_array[k]].id, 8); 
                  res = (char *)res + 8; 
              }
              for(unsigned int k = recovery_index; k < pos_array.size(); k++) {
                  int tid = pos_array[k] / 1500000;
                  uint64_t pos = (pos_array[k] % 1500000) * 272UL;
                  // printf("get salary in %d pos = %d\n", tid, pos);
                  memcpy(res, pmem_address_[tid] + pos + 264, 8); 
                  res = (char *)res + 8; 
              }
              break;
          default: break; // wrong
      }
  }

  #if DEBUG
  read_count = read_count + 1;
  if(read_count % 1000000 == 0) {
    std::cout << "read count " << read_count << std::endl;
    gettimeofday(&t2,NULL);
    print_time(t1, t2);
  } 
  #endif
  return pos_array.size();
}


Status NvmDB::WriteMemory(const char *tuple, size_t len, uint32_t current_index) {

  memcpy(&table[current_index], tuple, len);
  // std::cout << i << "current_index = " << current_index << std::endl;
  id_to_pos_[0].insert(std::pair<uint64_t, uint32_t>(table[current_index].id, current_index));
  salary_to_pos_[0].insert(std::pair<int64_t, uint32_t>(table[current_index].id, current_index));
  hashtable_put(userId_to_pos_, table[current_index].user_id, current_index);
  return 0;
}

Status NvmDB::Recovery() {

  gettimeofday(&t1,NULL);

  write_count = 0;
  read_count = 0;

  User user;
  uint64_t recovery_pos = 0UL;

  for(int i = 0; i < PMEM_FILE_COUNT; i++) {
    memcpy(&user, pmem_address_[i] + recovery_pos, 272UL);
  //   print_user(user);
    while(user.id != 0) {
        recovery_pos += 272UL;
        WriteMemory((char *)(&user), 272UL, write_count);
        memcpy(&user, pmem_address_[i] + recovery_pos, 272UL);
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
    // printf("file %d repos = %lu\n",i, recovery_pos);
    recovery_pos = 0UL;
  }
  printf("recovery %d tuples \n", write_count);
  write_count = 0;
  gettimeofday(&t2,NULL);
  print_time(t1, t2);
  return 0;
}