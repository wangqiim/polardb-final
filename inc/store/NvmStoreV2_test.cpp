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

static void storage_assert(bool condition, const std::string &msg) {
  if (!condition) {
    std::cout << msg << std::endl;
    // spdlog::error("[storage] message = {}", msg);
    exit(1);
  }
}

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

int main() {
    {
        uint8_t default_value = 0xFF;
        auto mem_res = must_init_mmem_file("./test_mem", 1024 * 1024, default_value);
        std::cout << uint64_t(mem_res.first) << std::endl;
        std::cout << mem_res.second << std::endl;
        for (int i = 0; i < 1024*1024; i++) {
            assert(*(uint8_t *)(mem_res.first + i) == default_value);
        }
    }
    { 
        uint8_t default_value = 0;
        auto pmem_res = must_init_pmem_file("./test_pmem", 1024 * 1024, default_value);
        std::cout << uint64_t(pmem_res.first) << std::endl;
        std::cout << pmem_res.second << std::endl;
        for (int i = 0; i < 1024*1024; i++) {
            assert(*(uint8_t *)(pmem_res.first + i) == default_value);
        }
    }
   
}