#include <cstdio>
#include <libpmem.h>
#include <iostream>
#include <string>
#include <unistd.h>
#include <sys/time.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <atomic>
#include <mutex>
#include <assert.h>
#include <cstring>
#include "NvmStoreV2.h"

int main() {
  const char* aep_dir ="/pmem/aep/";
  const char* disk_dir ="/tmp/disk/";
  initStore(aep_dir, disk_dir);
  std::cout << "hello world!" << std::endl;
  char tuple[RECORD_SIZE];
  for (uint64_t i = 0; i < 100; i++) {
    *(uint64_t *)tuple = i;
    *(uint64_t *)(tuple + 264) = i;
    writeTuple(tuple, RECORD_SIZE);
  }
  for (uint64_t i = 10000 * 50; i < 10000 * 50 + 100; i++) {
    *(uint64_t *)tuple = i;
    *(uint64_t *)(tuple + 264) = i;
    writeTuple(tuple, RECORD_SIZE);
  }

  char res[128];
  for (uint64_t i = 0; i < 100; i++) {
    readColumFromPos(0, i, res);
    assert(*(uint64_t *)(res) == i);
    readColumFromPos(3, i, res);
    assert(*(uint64_t *)(res) == i);
  }
  for (uint64_t i = 10000 * 50; i < 10000 * 50 + 100; i++) {
    readColumFromPos(0, (i - 10000 * 50) | 0x80000000U, res);
    assert(*(uint64_t *)(res) == i);
    readColumFromPos(3, (i - 10000 * 50) | 0x80000000U, res);
    assert(*(uint64_t *)(res) == i);
  }
}

// rm /tmp/disk/.meta && rm /tmp/disk/.mem_data  && rm /pmem/aep/.pmem_data && rm /pmem/aep/.pmem_random