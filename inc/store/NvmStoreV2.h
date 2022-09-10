#pragma once
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
#include "spdlog/spdlog.h"

#include "Config.h"
#include "./index/MemIndex.h"

std::mutex rand_write_mtx;
static void storage_assert(bool condition, const std::string &msg) {
  if (!condition) {
    spdlog::error("[storage] message = {}", msg);
    exit(1);
  }
}

/**
 * 数据的组织结构有3个文件组成, pmem_data_file + mmem_data_file + mmem_meta_file
 * mmem_meta_file: 记录meta信息，比如pmem_data_file的起始偏移，比如slot_0的id可能是0或2e或4e或6e
 * mmem_data_file: (salary,commitFlag)写在mmem_data_file上
 * pmem_data_file: 其中user_id和name写在pmem_data_file上
 * pmem_random_file: 一个大pmem进行互斥append, 用来处理测试阶段的随机写入, 不care性能。比如写入id=0,id=99999999999999
 */

/**
 * mmem_meta_file 上数据仅存储一个偏移号比如offset: 0, 2e, 4e, 8e
 * 取1则表示mem_data_file中的每一个slot_id对应的数据ID需要加上 offset
 * ---------------------------
 * | offset  | .. unused ..
 * ---------------------------
 * | uint_64 | .. unused ..
 * ---------------------------
 * 
 */
struct {
  char *address = nullptr;
  int64_t *offset = nullptr;
} MmemMeta;

/**
 * mmem_data_file 上数据仅存储salary, commit_flag. (uint64_t, uint64_t), (注意：commit_flag无法去除，因为测试样例中salary会存在插入0的情况.)
 * commit_flag=0表示该数据无效，comit_flag = 0xFFFFFFFF表示该数据已经提交
 * -----------------------------------------------------------------------------
 * | slot_0 (id = 0+off))  | slot_1 (id = 1+off))  | slot_2 (id = 2+off))  | 
 * -----------------------------------------------------------------------------
 * | (salary, commit_flag) | (salary, commit_flag) | (salary, commit_flag) | 
 * -----------------------------------------------------------------------------
 * 
 */
struct {
  char *address = nullptr;
} MmemData;
/**
 * pmem_data_file 上数据仅存储user_id和name，正好256字节，可以打满pmem的写入带宽
 * ---------------------------------------------------------------------
 * | slot_0 (id = 0+off) | slot_1 (id = 1+off) | slot_2 (id = 2+off) | 
 * ---------------------------------------------------------------------
 * |   (user_id, name)   |   (user_id, name)   |   (user_id, name)   |
 * ---------------------------------------------------------------------
 * 
 */
struct {
  char *address = nullptr;
} PmemData;

/**
 * pmem_random_file 上数据存储commit_cnt和append完整记录，每次写入是互斥的。
 * --------------------------------------------------------------------
 * |   8 bytes  | slot_0            |        slot_1       | ... | ... 
 * --------------------------------------------------------------------
 * | commit_cnt | tuple(272bytes)   |   tuple(272bytes)   | ... | ... 
 * ---------------------------------------------------------------------
 * 
 */
struct {
  char *address = nullptr;
  uint64_t *commit_cnt;
} PmemRandom;

const uint64_t CommitFlag = 0xFFFFFFFFFFFFFFFFUL;
const uint64_t MmemMetaFileSIZE = 8;          // 目前仅仅存储(offset)
const uint64_t MmemDataFileSIZE = (8 + 8) * TOTAL_WRITE_NUM;    // 2亿条 (salary+commitFlag)
const uint64_t PmemDataFileSIZE = 256 * TOTAL_WRITE_NUM;  // 2亿条 (user_id, name)
const uint64_t PmemRandomFileSIZE = 8 + RECORD_SIZE * TOTAL_WRITE_NUM;  // commit_cnt(8 bytes) + 2亿条完整记录，用来handle随机写入

// 该函数不会失败，否则panic
std::pair<char*, bool> must_init_mmem_file(const std::string path, uint64_t mmap_size) {
  bool is_create = false;
  int fd = -1;
  if (access(path.c_str(), F_OK) != 0) { // 不存在,则创建文件
    is_create = true;
    fd = open(path.c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
    storage_assert(fd >= 0, "create mmem file fail");
    storage_assert(lseek(fd, mmap_size - 1, SEEK_SET) != -1, "lseek fail");
    storage_assert(write(fd, "0", 1) == 1, "append '0' to mmemfile fail");
  } else {
    fd = open(path.c_str(), O_RDWR);
    storage_assert(fd >= 0, "open exist mmem file fail");
  }
  char *ptr = (char *)mmap(0, mmap_size, PROT_WRITE | PROT_READ, MAP_SHARED, fd, 0);
  storage_assert(ptr != nullptr, "mmap fail");
  if (is_create) {
    memset(ptr, 0, mmap_size);
  }
  return {ptr, is_create};
}

// 该函数不会失败，否则panic
std::pair<char*, bool> must_init_pmem_file(const std::string path, uint64_t mmap_size) {
  bool is_create = false;
  if (access(path.c_str(), F_OK) != 0) { // 不存在,则创建文件
    is_create = true;
  }
  int is_pmem;
  size_t mapped_len;
  char *ptr = (char *)pmem_map_file(path.c_str(), mmap_size, PMEM_FILE_CREATE, 0666, &mapped_len, &is_pmem);
  storage_assert(ptr != nullptr, "pmem_map_file fail");
  if (is_create) {
    pmem_memset_nodrain(ptr, 0, mmap_size);
  }
  return {ptr, is_create};
}

static void writeTuple(const char *tuple, __attribute__((unused)) size_t len, uint8_t tid) {
  uint64_t id = *(uint64_t *)tuple;
  auto atomic_offset = reinterpret_cast<std::atomic<int64_t> *>(MmemMeta.offset);
  auto old_value = atomic_offset->load();
  int64_t new_value = id / TOTAL_WRITE_NUM * TOTAL_WRITE_NUM; // 0, 1, 2, 3以外的值都是随机写入
  
  if (old_value == -1 && new_value >= 0 && new_value <= 6e8) {
      // 必须是属于0, 1, 2, 3 即 [0, 8e-1]范围内的id
    while (!atomic_offset->compare_exchange_strong(old_value, new_value)) { // cas保证只会被初始化一次offset
      if (old_value != -1) {
        break;
      }
    }
  }

  if (*MmemMeta.offset == new_value) {
    // 1. userId, name 写入pmem
    pmem_memcpy_nodrain(PmemData.address+256*(id-*MmemMeta.offset), tuple + 8, 256);
    // 2. salary写入mmem
    char *mmem_data_ptr = MmemData.address+16*(id-*MmemMeta.offset);
    memcpy(mmem_data_ptr, tuple+264, 4);
    // 3. commitFlag写入mmem
    *(uint64_t *)(mmem_data_ptr + 8) = CommitFlag;
    pmem_drain();
  } else {
    std::lock_guard<std::mutex> lock(rand_write_mtx);
    pmem_memcpy_nodrain(PmemRandom.address + (*PmemRandom.commit_cnt * RECORD_SIZE), tuple, RECORD_SIZE);
    *PmemRandom.commit_cnt = *PmemRandom.commit_cnt + 1;
    pmem_drain();
  }
}

static void readColumFromPos(int32_t select_column, uint32_t pos, void *res) {
  if (select_column == Id) {
    uint64_t id = *MmemMeta.offset + pos;
    memcpy(res, &id, 8);
    return;
  }
  if (select_column == Userid) {
    char *pmem_data_ptr = PmemData.address + pos * (256);
    memcpy(res, pmem_data_ptr, 128);
    return;
  }
  if (select_column == Name) {
    char *pmem_data_ptr = PmemData.address + pos * (256);
    memcpy(res, pmem_data_ptr + 128, 128);
    return;
  }
  if (select_column == Salary) {
    char *mmem_data_ptr = MmemData.address + pos * (16);
    memcpy(res, mmem_data_ptr, 8);
    return;    
  }
}

// build index
static void recovery() {
  uint64_t recovery_cnt = 0;
  unsigned char tuple[RECORD_SIZE];
  // 1. 恢复正常区域的数据
  if (*MmemMeta.offset >= 0) {
    for (uint64_t i = 0; i < TOTAL_WRITE_NUM; i++) {
      char *mmem_data_ptr = MmemData.address + 16 * i;
      uint64_t commit_flag = *(uint64_t *)(mmem_data_ptr + 8);
      if (commit_flag != 0) {
        uint64_t id = i + *MmemMeta.offset;
        char *pmem_data_ptr = PmemData.address + 256 * i;
        memcpy(tuple, &id, 8);
        memcpy(tuple + 8, pmem_data_ptr, 256);
        memcpy(tuple + 264, mmem_data_ptr, 8);
        recovery_cnt++;
      }
      insert((const char *)tuple, RECORD_SIZE, i);
    }
  }
  // 2. 恢复random写入区域的数据
  uint64_t commit_cnt = *PmemRandom.commit_cnt;
  for (uint64_t i = 0; i < commit_cnt; i++) {
    memcpy(tuple, PmemRandom.address + commit_cnt*RECORD_SIZE, RECORD_SIZE);
    insert((const char *)tuple, RECORD_SIZE, 2e8 + i); // 正常写入的id会被取余到[0, 2e8-1]的范围，因此slot号可以通过2e8为分界线进行区分
    recovery_cnt++;
  }
  spdlog::info("recovery success, recovery {} tuples", recovery_cnt);
}

static void init_storage(const std::string &mmem_meta_filename,
    const std::string &mmem_data_filename, const std::string &pmem_data_filename, const std::string &pmem_random_filename) {
  // 1. 创建/打开 mmem meta file, 如果是创建，则初始化该文件，并且offset置为-1
  {
    auto res = must_init_mmem_file(mmem_meta_filename, MmemMetaFileSIZE);
    MmemMeta.address = res.first;
    MmemMeta.offset = (int64_t *)res.first;
    if (res.second) { // 新文件
      *(MmemMeta.offset) = -1;
    }
  }
  // 2. 创建/打开 mmem data file, 如果是创建，则初始化该文件
  {
    auto res = must_init_mmem_file(mmem_data_filename, MmemDataFileSIZE);
    MmemData.address = res.first;
  }
  // 3. 创建/打开 pmem data file, 如果是创建，则初始化该文件
  {
    auto res = must_init_pmem_file(pmem_data_filename, PmemDataFileSIZE);
    PmemData.address  = res.first;
  }
  // 3. 创建/打开 pmem random file, 如果是创建，则初始化该文件
  {
    auto res = must_init_pmem_file(pmem_random_filename, PmemRandomFileSIZE);
    PmemRandom.commit_cnt = (uint64_t *)res.first;
    PmemRandom.address  = res.first + 8;
  }
}

static void initStore(const char* aep_dir,  const char* disk_dir) {
  spdlog::info("get sys pmem dir: {} ssd dir: {}", aep_dir, disk_dir);
  std::string disk_dir_str = std::string(disk_dir);
  std::string aep_dir_str = std::string(aep_dir);
  std::string mmem_meta_filename = disk_dir_str + ".meta"; // todo(wq): 注意是否需要再.meata前面一个加'/'
  std::string mmem_data_filename = disk_dir_str + ".data";
  std::string pmem_data_filename = aep_dir_str  + ".data";
  std::string pmem_random_filename = aep_dir_str  + ".random";

  init_storage(mmem_meta_filename, mmem_data_filename, pmem_data_filename, pmem_random_filename);
  recovery();
  spdlog::info("Store Init End");
}
