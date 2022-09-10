#pragma once
#include <cstdio>
#include <libpmem.h>
#include <iostream>
#include <string>
#include <unistd.h>
#include <sys/time.h>
#include <sys/mman.h>

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
}PBM[PMEM_FILE_COUNT];

struct MemBlockMeta {
  char *address = nullptr;
  uint64_t offset = 0;
}MBM[PMEM_FILE_COUNT];

static inline void print_time(struct timeval t1, struct timeval t2) {
  double timeuse = (t2.tv_sec - t1.tv_sec) + (double)(t2.tv_usec - t1.tv_usec)/1000000.0;
  spdlog::info("time = {}s", timeuse);
}

static void recovery();

static void create_metal(std::string path, int i, bool is_pmem) {
    if (is_pmem) {
        path = path + "pmem" + std::to_string(i) + ".pool";
    } else {
        path = path + "mem" + std::to_string(i) + ".pool";
    }
    bool is_create = true;
    int fd;
    size_t mmap_size = MEM_RECORD_SIZE * PER_THREAD_MAX_WRITE + COMMIT_FLAG_SIZE;
    if (access(path.c_str(), F_OK) == 0) {
        is_create = false;
        if(!is_pmem) {
            fd = open(path.c_str(), O_RDWR);
            lseek(fd, mmap_size - 1, SEEK_SET);
        }
    } else {
        if(!is_pmem) {
            fd = open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
            lseek(fd, mmap_size - 1, SEEK_SET);
            if (write(fd, "0", 1) < 0) {
              spdlog::error("write error");
            }
        }
    }
    if (is_pmem) {
        mmap_size = PMEM_RECORD_SIZE * PER_THREAD_MAX_WRITE;
        int is_pmem;
        size_t mapped_len;
        /* create a pmem file and memory map it */
        if ( (PBM[i].address = (char *)pmem_map_file(path.c_str(), mmap_size, PMEM_FILE_CREATE,
                                                     0666, &mapped_len, &is_pmem)) == NULL ) {
          spdlog::error("pmem_map_file");
        }
    } else {
      MBM[i].address = (char *)mmap(0, mmap_size, PROT_WRITE | PROT_READ, MAP_SHARED, fd, 0);
      MBM[i].address += COMMIT_FLAG_SIZE;
    }
    if (is_create) {
        if (is_pmem) {
            pmem_memset_nodrain(PBM[i].address, 0, mmap_size);
        } else {
            memset(MBM[i].address - COMMIT_FLAG_SIZE, 0, mmap_size);
            close(fd);
        }
    }
}

static void initStore(const char* aep_dir,  const char* disk_dir) {
  spdlog::info("get sys pmem dir: {} ssd dir: {}", aep_dir, disk_dir);
  for (uint64_t i = 0; i < PMEM_FILE_COUNT; i++) {
      create_metal(std::string(aep_dir), i, true);
      create_metal(std::string(disk_dir), i, false);
  }
  recovery();
  spdlog::info("Store Init End");
}

static void writeTuple(const char *tuple, __attribute__((unused)) size_t len, uint8_t tid) {
  memcpy(MBM[tid].address + MBM[tid].offset, tuple, 8);
  memcpy(MBM[tid].address + MBM[tid].offset + 8, tuple + 264, 8);
  pmem_memcpy_nodrain(PBM[tid].address + PBM[tid].offset, tuple + 8, 128);
  pmem_memcpy_nodrain(PBM[tid].address + PBM[tid].offset + 128, tuple + 136, 128);
  uint32_t pos = (PBM[tid].offset / PMEM_RECORD_SIZE) + 1;
  memcpy(MBM[tid].address - 4, &pos, 4);
  pmem_drain();
  PBM[tid].offset += PMEM_RECORD_SIZE;
  MBM[tid].offset += MEM_RECORD_SIZE;
}

static void readColumFromPos(int32_t select_column, uint32_t pos, void *res) {
  uint8_t tid = pos / PER_THREAD_MAX_WRITE;
  uint64_t offset = (pos % PER_THREAD_MAX_WRITE);
  if (select_column == Id) {
    memcpy(res, MBM[tid].address + offset * MEM_RECORD_SIZE, 8);
    return;
  }
  if (select_column == Userid) {
    memcpy(res, PBM[tid].address + offset * PMEM_RECORD_SIZE, 128);
    return;
  }
  if (select_column == Name) {
    memcpy(res, PBM[tid].address + offset * PMEM_RECORD_SIZE + 128, 128);
    return;
  }
  if (select_column == Salary) {
    memcpy(res, MBM[tid].address + offset * MEM_RECORD_SIZE + 8, 8);
    return;    
  }
}

// build index
static void recovery() {
  uint64_t recovery_cnt = 0;
  for (uint64_t i = 0; i < PMEM_FILE_COUNT; i++) {
    uint32_t commit_cnt = *(uint32_t *)(MBM[i].address - 4);
    for (uint64_t j = 0; j < commit_cnt; j++) {
      unsigned char tuple[RECORD_SIZE];
      memcpy(tuple, MBM[i].address + MBM[i].offset, 8);
      memcpy(tuple + 8, PBM[i].address + PBM[i].offset, 128);
      memcpy(tuple + 136, PBM[i].address + PBM[i].offset + 128, 128);
      memcpy(tuple + 264, MBM[i].address + MBM[i].offset + 8, 8);
      insert((const char *)tuple, RECORD_SIZE, i);
      PBM[i].offset += PMEM_RECORD_SIZE;
      MBM[i].offset += MEM_RECORD_SIZE;
    }
    recovery_cnt += PBM[i].offset/PMEM_RECORD_SIZE;
  }
  spdlog::info("recovery {} tuples", recovery_cnt);
  spdlog::info("RECOVERY END");
}

