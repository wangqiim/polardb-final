#pragma once
#include <cstdio>
#include <libpmem.h>
#include <iostream>
#include <string>
#include <unistd.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <set>

#include "Config.h"
#include "./index/MemIndexV2.h"

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
}PBM, PRBM;

struct MemBlockMeta {
    char *address = nullptr;
}MBM;

static std::atomic<uint32_t> write_count(0);
uint32_t mod = (1<<28) - 1;

static inline void print_time(struct timeval t1, struct timeval t2) {
  double timeuse = (t2.tv_sec - t1.tv_sec) + (double)(t2.tv_usec - t1.tv_usec)/1000000.0;
  spdlog::info("time = {}s", timeuse);
}

static void recovery();

static void create_mem_metal(MemBlockMeta &meta, std::string path, size_t mmap_size) {
  bool is_create = true;
  int fd;
  if (access(path.c_str(), F_OK) == 0) {
    is_create = false;
    fd = open(path.c_str(), O_RDWR);
    lseek(fd, mmap_size - 1, SEEK_SET);
  } else {
    fd = open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    lseek(fd, mmap_size - 1, SEEK_SET);
    if (write(fd, "0", 1) < 0) {
      spdlog::error("write error");
    }
  }
  meta.address = (char *)mmap(0, mmap_size, PROT_WRITE | PROT_READ, MAP_SHARED, fd, 0);
  if (is_create) {
    memset(meta.address, 0, mmap_size);
  }
  close(fd);
}

static void create_pmem_metal(PmemBlockMeta &meta, std::string path, size_t mmap_size) {
  bool is_create = true;
  if (access(path.c_str(), F_OK) == 0) {
    is_create = false;
  }
  mmap_size = PMEM_RECORD_SIZE * TOTAL_WRITE_NUM;
  int is_pmem;
  size_t mapped_len;
  /* create a pmem file and memory map it */
  if ( (meta.address = (char *)pmem_map_file(path.c_str(), mmap_size, PMEM_FILE_CREATE,
                                                 0666, &mapped_len, &is_pmem)) == NULL ) {
    spdlog::error("pmem_map_file");
  }
  if (is_create) {
    pmem_memset_nodrain(meta.address, 0, mmap_size);
  }
}

static void initStore(const char* aep_dir,  const char* disk_dir) {
  spdlog::info("get sys pmem dir: {} ssd dir: {}", aep_dir, disk_dir);
  create_mem_metal(MBM, std::string(disk_dir) + "mem.pool", 9 * TOTAL_WRITE_NUM + COMMIT_FLAG_SIZE);
  create_pmem_metal(PBM, std::string(aep_dir) + "pmem.pool", PMEM_RECORD_SIZE * TOTAL_WRITE_NUM);
  create_pmem_metal(PRBM ,std::string(aep_dir) + "rpmem.pool", RECORD_SIZE * 1000000);
  MBM.address += COMMIT_FLAG_SIZE;
  PRBM.address += COMMIT_FLAG_SIZE;
  recovery();
  spdlog::info("Store Init End");
}

std::mutex rid_mutex;
static void writeTuple(const char *tuple, __attribute__((unused)) size_t len, uint8_t tid) {
  uint64_t id = *(uint64_t *)tuple;
  uint64_t index = id & mod;
  if (index >= 2e8) {
    //互斥存储
    rid_mutex.lock();
    pmem_memcpy_nodrain(PRBM.address + PRBM.offset * RECORD_SIZE, tuple, len);
    PRBM.offset++;
    pmem_memcpy_nodrain(PRBM.address - 4, &PRBM.offset, 4);
    rid_mutex.unlock();
  } else {
    pmem_memcpy_nodrain(PBM.address + index * PMEM_RECORD_SIZE, tuple + 8, 256);
    pmem_drain();

    memcpy(MBM.address + index * 9, tuple + 264, 8);
    uint8_t flag = 1;
    memcpy(MBM.address + index * 9 + 8, &flag, 1);

    write_count++;
    memcpy(MBM.address - 4, &write_count, 4);
  }
}

static void readColumFromPos(int32_t select_column, uint64_t id, void *res) {
  uint64_t index = id & mod;
  if (index >= 2e8) {
    for (uint32_t i = 0; i < 1000000; i++) {
      User *user = (User *)(PRBM.address + i * RECORD_SIZE);
      if (user->id == id) {
        if (select_column == Id) {
          memcpy(res, &user->id, 8);
          return;
        }
        if (select_column == Userid) {
          memcpy(res, user->user_id, 128);
          return;
        }
        if (select_column == Name) {
          memcpy(res, user->name, 128);
          return;
        }
        if (select_column == Salary) {
          memcpy(res, &user->salary, 8);
          return;
        }
      }
    }
    return;
  }
  if (select_column == Id) {
    memcpy(res, &id, 8);
    return;
  }
  if (select_column == Userid) {
    memcpy(res, PBM.address + index * PMEM_RECORD_SIZE, 128);
    return;
  }
  if (select_column == Name) {
    memcpy(res, PBM.address + index * PMEM_RECORD_SIZE + 128, 128);
    return;
  }
  if (select_column == Salary) {
    memcpy(res, MBM.address + index * 9, 8);
    return;
  }
}

// build index
static void recovery() {
  uint64_t recovery_cnt = 0;
  //正常的数量
  uint32_t commit_cnt = *(uint32_t *)(MBM.address - 4);
  //恢复随机id的数量
  uint32_t rid_cont = *(uint32_t *)(PRBM.address - 4);
  for (uint32_t i = 0; i < rid_cont; i++) {
    User *user = (User *)(PRBM.address + i * RECORD_SIZE);
    insert((const char *)user, RECORD_SIZE, 0);
    PRBM.offset++;
    recovery_cnt++;
  }
  spdlog::info("current count {} need recovery {} tuple", recovery_cnt, commit_cnt);
  for (uint64_t index = 0; index < TOTAL_WRITE_NUM; index++) {
    //空数据的跳过
    if ((*(uint8_t *)(MBM.address + index * 9 + 8)) != 0) {
      unsigned char tuple[RECORD_SIZE];
      memcpy(tuple, &index, 8);
      memcpy(tuple + 8, PBM.address + index * PMEM_RECORD_SIZE, 256);
      memcpy(tuple + 264, MBM.address + index * 9, 8);
      insert((const char *) tuple, RECORD_SIZE, 0);
      recovery_cnt++;
      if (recovery_cnt >= commit_cnt + rid_cont) break;
    }
  }
  spdlog::info("recovery {} tuples", recovery_cnt);
  spdlog::info("RECOVERY END");
}

