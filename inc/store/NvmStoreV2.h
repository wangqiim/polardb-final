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

static void storage_assert(bool condition, const std::string &msg) {
  if (!condition) {
    spdlog::error("[storage] message = {}", msg);
    exit(1);
  }
}

static bool IsNormalPos(uint64_t pos) {
  return (pos & 0x80000000U) == 0;
}

/**
 * 数据的组织结构有3个文件组成, pmem_data_file + mmem_data_file + mmem_meta_file
 * mmem_meta_file: 记录meta信息，比如pmem_data_file的起始偏移，比如slot_0的id可能是0或2e或4e或6e
 * mmem_data_file: (salary,commitFlag)写在mmem_data_file上
 * pmem_data_file: 其中user_id和name写在pmem_data_file上
 * pmem_random_file: 用来处理正确性阶段的随机id。一个大的pmem，根据写入线程的id对每个线程写入的数据分块
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
  uint64_t *offset = nullptr;
  std::mutex mtx_;
} MmemMeta;

/**
 * mmem_data_file 上数据仅存储salary + 1. uint64_t, (注意：salary需要加1作为commit标记(测试数据有salary = 0).)
 * salary=0xFFFF FFFF FFFF FFFF表示该数据无效，否则表示该数据已提交
 * -------------------------------------------------------------------------
 * | slot_0 (id = 0+off))  | slot_1 (id = 1+off))  | slot_2 (id = 2+off))  | 
 * -------------------------------------------------------------------------
 * |        salary         |        salary         |        salary         | 
 * -------------------------------------------------------------------------
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
 * pmem_random_file 上数据存储commit_cnt和append完整记录，每个写入线程各有一个。
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
} PmemRandom[PMEM_FILE_COUNT];

const uint64_t MmemMetaFileSIZE = 8;          // 目前仅仅存储(offset)
const uint64_t MmemDataFileSIZE = 8 * TOTAL_WRITE_NUM;    // 2亿条salary
const uint64_t PmemDataFileSIZE = 256 * TOTAL_WRITE_NUM;  // 2亿条 (user_id, name)

// (commit_cnt(8 bytes) + 100W) * 50，用来handle正确性阶段的随机id写入，每个线程不会写超过100万条数据
const uint64_t PmemRandRecordNumPerThread = 1000000; // 400W太大，pmem不够放
const uint64_t PmemRandomFileSIZEPerThread = 8 + RECORD_SIZE * PmemRandRecordNumPerThread;
const uint64_t TotalPmemRandomFileSIZE = PmemRandomFileSIZEPerThread * PMEM_FILE_COUNT;

// const uint64_t MmemMetaFileSIZE = 8;          // 目前仅仅存储(offset)
// const uint64_t MmemDataFileSIZE = 8 * TOTAL_WRITE_NUM;    // 2亿条salary
// const uint64_t PmemDataFileSIZE = 256 * TOTAL_WRITE_NUM;  // 2亿条 (user_id, name)

// // (commit_cnt(8 bytes) + 100W) * 50，用来handle正确性阶段的随机id写入，每个线程不会写超过100万条数据
// const uint64_t PmemRandRecordNumPerThread = 10000; // 400W太大，pmem不够放
// const uint64_t PmemRandomFileSIZEPerThread = 8 + RECORD_SIZE * PmemRandRecordNumPerThread;
// const uint64_t TotalPmemRandomFileSIZE = PmemRandomFileSIZEPerThread * PMEM_FILE_COUNT;

// 该函数不会失败，否则panic
std::pair<char*, bool> must_init_mmem_file(const std::string path, uint64_t mmap_size, uint8_t default_val) {
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
    memset(ptr, default_val, mmap_size);
  }
  return {ptr, is_create};
}

// 该函数不会失败，否则panic
std::pair<char*, bool> must_init_pmem_file(const std::string path, uint64_t mmap_size, uint8_t default_val) {
  bool is_create = false;
  if (access(path.c_str(), F_OK) != 0) { // 不存在,则创建文件
    is_create = true;
  }
  int is_pmem;
  size_t mapped_len;
  char *ptr = (char *)pmem_map_file(path.c_str(), mmap_size, PMEM_FILE_CREATE, 0666, &mapped_len, &is_pmem);
  storage_assert(ptr != nullptr, "pmem_map_file fail");
  if (is_create) {
    pmem_memset_nodrain(ptr, default_val, mmap_size);
  }
  return {ptr, is_create};
}

static std::atomic<int8_t> StoreTid(0);
// 1. 写入数据
// 2. 写入索引
static void writeTuple(const char *tuple, size_t len) {
  static thread_local int8_t tid = -1;
  uint64_t id = *(uint64_t *)tuple;
  if (tid == -1) {
    tid = StoreTid++;
    std::lock_guard<std::mutex> guard(MmemMeta.mtx_);
    if (*MmemMeta.offset == 0xFFFFFFFFFFFFFFFFUL) { // 0xFFFFFFFFFFFFFFFFUL 表示offset还未初始化
      if (id < uint64_t(8e8)) {
        *MmemMeta.offset = (id / TOTAL_WRITE_NUM) * TOTAL_WRITE_NUM; // 0 or 2e8 or 4e8 or 6e8
      } else {
        *MmemMeta.offset = 0;
      }
      id_range.first = uint64_t(*MmemMeta.offset);
      id_range.second = id_range.first + TOTAL_WRITE_NUM;
    }
  }

  // 如果id属于[id_range.first, id_range.second), 则写入mem+pmem
  // 否则，写入rand_pmem区域
  if (id_range.first <= id && id < id_range.second) {
    uint64_t pos = id - id_range.first;
    // 1. userId, name 写入pmem
    pmem_memcpy_nodrain(PmemData.address + 256 * pos, tuple + 8, 256);
    // 2. salary写入mmem
    memcpy(MmemData.address + 8 * pos, tuple + 264, 8);
    pmem_drain();
    insert_idx(tuple, len, pos);
  } else {
    uint64_t rel_pos = *PmemRandom[tid].commit_cnt; // 该段数据上的相对偏移
    pmem_memcpy_nodrain(PmemRandom[tid].address + (rel_pos * RECORD_SIZE), tuple, RECORD_SIZE);
    pmem_drain();
    if (rel_pos == PmemRandRecordNumPerThread) {
      spdlog::error("[writeTuple] randpmem write overflow!!! PmemRandRecordNumPerThread = {}", PmemRandRecordNumPerThread);
    }
    rel_pos++;
    pmem_memcpy_nodrain(PmemRandom[tid].commit_cnt, &rel_pos, 8);
    uint64_t abs_pos = tid * PmemRandRecordNumPerThread + (rel_pos - 1);
    abs_pos = abs_pos | 0x80000000U; // 将最高位置为1，用以区分是否是random id区域上的数据
    insert_idx(tuple, len, abs_pos);
  }
}

// is_normal = true 从pmem+mem上读
// is_normal = false 从rand_pmem上读
static void readColumFromPos(int32_t select_column, uint64_t pos, void *res) {
  if (IsNormalPos(pos)) { // 最高位是0
    if (select_column == Id) {
      uint64_t id = pos + id_range.first;
      memcpy(res, &id, 8);
      return;
    }
    if (select_column == Userid) {
      char *pmem_data_ptr = PmemData.address + pos * 256;
      memcpy(res, pmem_data_ptr, 128);
      return;
    }
    if (select_column == Name) {
      char *pmem_data_ptr = PmemData.address + pos * 256;
      memcpy(res, pmem_data_ptr + 128, 128);
      return;
    }
    if (select_column == Salary) {
      char *mmem_data_ptr = MmemData.address + pos * 8;
      memcpy(res, mmem_data_ptr, 8);
      return;    
    }
  } else {
    pos = 0x7FFFFFFFU & pos;
    uint64_t tid = pos / PmemRandRecordNumPerThread;
    uint64_t rel_pos = pos % PmemRandRecordNumPerThread;
    if (select_column == Id) {
      char *pmem_data_ptr = PmemRandom[tid].address + rel_pos * RECORD_SIZE;
      memcpy(res, pmem_data_ptr, 8);
      return;
    }
    if (select_column == Userid) {
      char *pmem_data_ptr = PmemRandom[tid].address + rel_pos * RECORD_SIZE + 8;
      memcpy(res, pmem_data_ptr, 128);
      return;
    }
    if (select_column == Name) {
      char *pmem_data_ptr = PmemRandom[tid].address + rel_pos * RECORD_SIZE + 136;
      memcpy(res, pmem_data_ptr, 128);
      return;
    }
    if (select_column == Salary) {
      char *pmem_data_ptr = PmemRandom[tid].address + rel_pos * RECORD_SIZE + 264;
      memcpy(res, pmem_data_ptr, 8);
      return;    
    }
  }
}

static bool IsPosCommit(uint64_t pos) {
  uint64_t salary = *(uint64_t *)(MmemData.address + 8 * pos);
  const uint64_t UN_COMMIT_SALARY = 0xFFFFFFFFFFFFFFFFUL;
  return salary != UN_COMMIT_SALARY;
}

// build index
static void recovery() {
  uint64_t recovery_cnt = 0;
  unsigned char tuple[RECORD_SIZE];
  // 1. 恢复正常区域的数据
  if (*MmemMeta.offset != 0xFFFFFFFFFFFFFFFFUL) {
    for (uint64_t pos = 0; pos < TOTAL_WRITE_NUM; pos++) {
      if (IsPosCommit(pos)) {
        uint64_t id = pos + id_range.first;
        char *pmem_data_ptr = PmemData.address + 256 * pos;
        memcpy(tuple, &id, 8);
        memcpy(tuple + 8, pmem_data_ptr, 256);
        memcpy(tuple + 264, MmemData.address + 8 * pos, 8);
        recovery_cnt++;
        insert_idx((const char *)tuple, RECORD_SIZE, (uint32_t)pos);
      }
    }
  }
  spdlog::info("[recovery] recovery normal data({}) success!", recovery_cnt);
  // 2. 恢复random写入区域的数据
  for (uint64_t i = 0; i < PMEM_FILE_COUNT; i++) {
    uint64_t commit_cnt = *PmemRandom[i].commit_cnt;
    if (commit_cnt != 0) {
      spdlog::info("[recovery] randomPmem tid={}, commit_cnt={}!", i, commit_cnt);
    }
    for (uint64_t j = 0; j < commit_cnt; j++) {
      memcpy(tuple, PmemRandom[i].address + j*RECORD_SIZE, RECORD_SIZE);
      uint64_t abs_pos = PmemRandRecordNumPerThread * i + j;
      insert_idx((const char *)tuple, RECORD_SIZE, (1UL << 31) | abs_pos);
      recovery_cnt++;
    }
  }
  spdlog::info("recovery success, recovery {} tuples", recovery_cnt);
}

static void init_storage(const std::string &mmem_meta_filename,
    const std::string &mmem_data_filename, const std::string &pmem_data_filename, const std::string &pmem_random_filename) {
  // 1. 创建/打开 mmem meta file, 如果是创建，则初始化该文件，并且offset置为-1
  {
    uint8_t default_value = 0;
    auto res = must_init_mmem_file(mmem_meta_filename, MmemMetaFileSIZE, default_value);
    MmemMeta.address = res.first;
    MmemMeta.offset = (uint64_t *)res.first;
    if (res.second) { // 新文件
      *(MmemMeta.offset) = 0xFFFFFFFFFFFFFFFFUL; // 0xFFFFFFFFFFFFFFFFUL 表示offset还未初始化
    } else {
      id_range.first = uint64_t(*MmemMeta.offset);
      id_range.second = id_range.first + TOTAL_WRITE_NUM;
    }
  }
  // 2. 创建/打开 mmem data file, 如果是创建，则初始化该文件
  {
    uint8_t default_value = 0xFF;
    auto res = must_init_mmem_file(mmem_data_filename, MmemDataFileSIZE, default_value);
    MmemData.address = res.first;
  }
  // 3. 创建/打开 pmem data file, 如果是创建，则初始化该文件
  {
    uint8_t default_value = 0;
    auto res = must_init_pmem_file(pmem_data_filename, PmemDataFileSIZE, default_value);
    PmemData.address  = res.first;
  }
  // 3. 创建/打开 pmem random file, 如果是创建，则初始化该文件
  {
    uint8_t default_value = 0;
    auto res = must_init_pmem_file(pmem_random_filename, TotalPmemRandomFileSIZE, default_value);
    for (uint64_t i = 0; i < PMEM_FILE_COUNT; i++) {
      PmemRandom[i].address = res.first + (i * PmemRandomFileSIZEPerThread) + 8;
      PmemRandom[i].commit_cnt = reinterpret_cast<uint64_t *>(res.first + (i * PmemRandomFileSIZEPerThread));
    }
  }
}

static void initStore(const char* aep_dir,  const char* disk_dir) {
  spdlog::info("get sys pmem dir: {} ssd dir: {}", aep_dir, disk_dir);
  std::string disk_dir_str = std::string(disk_dir);
  std::string aep_dir_str = std::string(aep_dir);
  std::string mmem_meta_filename = disk_dir_str + "prefix.meta"; // todo(wq): 注意是否需要再.meata前面一个加'/'
  std::string mmem_data_filename = disk_dir_str + "prefix.mem_data";
  std::string pmem_data_filename = aep_dir_str  + "prefix.pmem_data";
  std::string pmem_random_filename = aep_dir_str  + "prefix.pmem_random";

  init_storage(mmem_meta_filename, mmem_data_filename, pmem_data_filename, pmem_random_filename);
  spdlog::info("[initStore] init_storage done!");
  recovery();
  spdlog::info("Store Init End");
}

static char *GetUserIdByPos(uint64_t pos) {
  char *pmem_data_ptr = nullptr;
  if (IsNormalPos(pos)) {
    pmem_data_ptr = PmemData.address + pos * 256;
  } else {
    pos = 0x7FFFFFFFU & pos;
    uint64_t tid = pos / PmemRandRecordNumPerThread;
    uint64_t rel_pos = pos % PmemRandRecordNumPerThread;
    pmem_data_ptr = PmemRandom[tid].address + rel_pos * RECORD_SIZE + 8;
  }
  return pmem_data_ptr;
}
