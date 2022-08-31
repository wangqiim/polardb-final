#pragma once
#include <cstdio>
#include <libpmem.h>
#include <iostream>
#include <string>
#include <unistd.h>
#include <sys/time.h>

#include "Config.h"
#include "./index/MemIndex.h"

class User
{
public:
    uint64_t id;
    unsigned char user_id[128];
    unsigned char name[128];
    uint64_t salary;
};

struct PmemBlockMeta {
  char *address = nullptr;
  uint64_t offset = 0;
}PBM[50];

static inline void print_time(struct timeval t1, struct timeval t2) {
  double timeuse = (t2.tv_sec - t1.tv_sec) + (double)(t2.tv_usec - t1.tv_usec)/1000000.0;
  spdlog::info("time = {}s", timeuse);
}

static void recovery();

static void initStore(const char* aep_dir, const char* disk_dir) {
  spdlog::info("get sys pmem dir: {}", aep_dir);
  std::string base_path = std::string(aep_dir);
  if(base_path[base_path.length() - 1] != '/') {
    base_path = base_path + "/";
  }
  unsigned long mmap_size = PMEM_SIZE / PMEM_FILE_COUNT;
  for(int i = 0; i < PMEM_FILE_COUNT; i++) {
    std::string path = std::string(base_path) + "pmem" + std::to_string(i) + ".pool";
    size_t mapped_len;
    int is_pmem;
    bool is_create = true;
    if (access(path.c_str(), F_OK) == 0) is_create = false;
    /* create a pmem file and memory map it */
    if ( (PBM[i].address = (char *)pmem_map_file(path.c_str(), mmap_size, PMEM_FILE_CREATE,
          0666, &mapped_len, &is_pmem)) == NULL ) {
      perror("pmem_map_file");
    }
    if (is_create) {
      pmem_memset_nodrain(PBM[i].address, 0, PMEM_SIZE / PMEM_FILE_COUNT);  
    }
    PBM[i].address += 4;
  }
  recovery();
  spdlog::info("Store Init End");
}



static void writeTuple(const char *tuple, size_t len, uint8_t tid) {
  pmem_memcpy_nodrain(PBM[tid].address + PBM[tid].offset, tuple, len);
  uint32_t pos = PBM[tid].offset / 272;
  pos++;
  pmem_memcpy_nodrain(PBM[tid].address - 4, &pos, 4);
  pmem_drain();
  PBM[tid].offset += len;
}

static void readColumFromPos(int32_t select_column, uint32_t pos, void *res) {
  uint8_t tid = pos / PER_THREAD_MAX_WRITE;
  uint64_t offset = pos % PER_THREAD_MAX_WRITE;
  char *user = PBM[tid].address + offset * 272UL;
  if (select_column == Id) {
    memcpy(res, user, 8);
    return;
  }
  if (select_column == Userid) {
    memcpy(res, user + 8, 128);
    return;
  }
  if (select_column == Name) {
    memcpy(res, user + 136, 128);
    return;
  }
  if (select_column == Salary) {
    memcpy(res, user + 264, 8);
    return;    
  }
}

// build index
static void recovery() {
  uint64_t recovery_cnt = 0;
  for (int i = 0; i < PMEM_FILE_COUNT; i++) {
    // while (*(uint64_t *)(PBM[i].address + PBM[i].offset) != 0xffffffffffffffff &&  PBM[i].offset/272 < PER_THREAD_MAX_WRITE) {
    //   insert(PBM[i].address + PBM[i].offset, 272UL, 0);
    //   PBM[i].offset += 272UL;
    // }
    for (int j = 0; j < *(uint32_t *)(PBM[i].address - 4); j++) {
      insert(PBM[i].address + PBM[i].offset, 272UL, i);
      PBM[i].offset += 272UL;
    }
    recovery_cnt += PBM[i].offset/272;
  }
  spdlog::info("recovery {} tuples", recovery_cnt);
  spdlog::info("RECOVERY END");
}

