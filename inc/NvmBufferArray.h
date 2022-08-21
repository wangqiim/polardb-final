#pragma once
#include <iostream>
#include <sys/mman.h>

#include <libpmem.h>
#include <xmmintrin.h>
#include <unistd.h> 
#include <string.h>
#include "./tools/RtMemcpy/rte_memcpy.h"

namespace com
{
	const static size_t _MAXSIZE_ = 80;
	extern void* (*g_base[_MAXSIZE_+1])(void *dest, const void *src);
};
 
inline void *xmemcpy(void *dest, const void *src, size_t len);
 
namespace com
{
	template <size_t size>
	struct xmemcpy_t
	{
		int data[size];
	};
 
	template <>
	struct xmemcpy_t<0>
	{
	};
 
	template <size_t size>
	class xmemcopy
	{
	public:
		inline static void * copy(void *dest, const void *src)
		{
			if (size > _MAXSIZE_)
			{
				size_t i = 0;
				for (; i + _MAXSIZE_ <= size; i += _MAXSIZE_)
					xmemcopy<_MAXSIZE_>::copy((char*)dest + i, (const char*)src + i);
				if (size % _MAXSIZE_) 
					xmemcopy<size % _MAXSIZE_>::copy((char*)dest + i, (const char*)src + i);
				return dest;
			}
			typedef xmemcpy_t<((size - 1) % _MAXSIZE_ + 1) / sizeof(int)> type_t;
			*((type_t *)dest) = *((type_t *)src);
			
			if ((size%sizeof(int)) > 0) {
				((char *)dest)[size - 1] = ((char *)src)[size - 1];
			}
			if ((size%sizeof(int)) > 1) {
				((char *)dest)[size - 2] = ((char *)src)[size - 2];
			}
			if ((size%sizeof(int)) > 2) {
				((char *)dest)[size - 3] = ((char *)src)[size - 3];
			}
			return dest;
		}
	};
 
	template <>
	class xmemcopy<0>
	{
	public:
		static void * copy(void *dest, const void *src) { return dest; }
	};
	
	void* (*g_base[_MAXSIZE_+1])(void *dest, const void *src);
 
	template <size_t len>
	void init() {
		g_base[len] = xmemcopy<len>::copy;
		init<len - 1>();
	}
 
	template <>
	void init<0>() {
		g_base[0] = xmemcopy<0>::copy;
	}
 
	struct xmem_monitor
	{
		xmem_monitor() 
		{
			init<_MAXSIZE_>();
		}
	};
 
	static xmem_monitor g_monitor;
}

const static uint32_t BigPageCount = 23;
const static uint32_t PageData =  16512UL;
const static uint32_t BigPageData = 258UL * 1070000U;

struct BigPage {
  char data[BigPageData];
  uint32_t offset;
};

struct Page {
    //271 条tuple
    char data[PageData];
    uint32_t offset;
};



const static int MemBlockCount = 50;
static char *memoryaddress[MemBlockCount];
static char *pmemaddress[MemBlockCount];

static BigPage *bigpages[BigPageCount];
static Page *pages[MemBlockCount];

//tool
static inline bool exists_test (const std::string& name) {
  std::ifstream f(name.c_str());
  return f.good();
}

static uint64_t over_offset[BigPageCount];
static uint64_t nomal_offset[MemBlockCount];

static void initNvmBuffer(const char* aep_dir, const char* disk_dir) {
  std::string base_path = std::string(aep_dir);
  if(base_path[base_path.length() - 1] != '/') {
    base_path = base_path + "/";
  }
  unsigned long mmap_size = PMEM_SIZE / MemBlockCount;
  int fd;
  for(int i = BigPageCount; i < MemBlockCount; i++) {
    std::string path = std::string(base_path) + "pmem" + std::to_string(i) + ".pool";
    size_t mapped_len;
    int is_pmem;
    bool is_create = true;
    if(exists_test(path)) {
      is_create = false;
    }
    /* create a pmem file and memory map it */
    if ( (pmemaddress[i] = (char *)pmem_map_file(path.c_str(), PMEM_SIZE / MemBlockCount, PMEM_FILE_CREATE,
          0666, &mapped_len, &is_pmem)) == NULL ) {
      perror("pmem_map_file");
    }
    if (is_create) {
      pmem_memset_nodrain(pmemaddress[i], 0, PMEM_SIZE / MemBlockCount);  
    }
  }

  std::string base_disk = std::string(disk_dir);
  if(base_disk[base_disk.length() - 1] != '/') {
    base_disk = base_disk + "/";
  } 

  //50个缓冲块
  std::string mem_path[MemBlockCount];
  std::cout << "Block Size: " << (PageData + 16) / 1024 / 1024 << "MB" << std::endl;
  std::cout << "Big Block Size: " << (BigPageData + 16) / 1024 / 1024 << "MB" << std::endl;
  for (uint32_t i = 0; i < MemBlockCount; i++) {
      mem_path[i] = base_disk + "disk"+ std::to_string(i) + ".txt";
      //BigPage不需要写PMEM
      if (i < BigPageCount) {mmap_size = BigPageData + 16; over_offset[i] = 1000000UL * 258UL;  }
      else mmap_size = PageData + 24;

      nomal_offset[i] = 0;
      bool is_create = false;
      if(exists_test(mem_path[i])) {
        fd = open(mem_path[i].c_str(), O_RDWR);
        lseek(fd,mmap_size - 1,SEEK_SET);
      } else {
        fd = open(mem_path[i].c_str(), O_RDWR | O_CREAT | O_TRUNC, 0644);
        lseek(fd, mmap_size - 1, SEEK_SET);
        write(fd, "0", 1);
        is_create = true;
      }
      if (fd == -1) {
        printf("Open file %s failed.\n", mem_path[i].c_str());
        exit(-1);
      }
      memoryaddress[i] = (char *)mmap(0, mmap_size, PROT_WRITE | PROT_READ, MAP_SHARED, fd, 0);
      if (memoryaddress[i] == MAP_FAILED) {
        printf("Map failed.\n");
        exit(-1);
      }
      if (i < BigPageCount) {
        bigpages[i] = (BigPage *)memoryaddress[i];
        if (is_create) {
          memset(bigpages[i], 0, sizeof(BigPage));
          bigpages[i] -> offset = 0;
        }       
      } else {
        pages[i] = (Page *)memoryaddress[i];
        if (is_create) {
          memset(pages[i], 0, sizeof(Page));
          pages[i] -> offset = 0;
        }
      }
      close(fd);
  }
}



pthread_mutex_t mem_mtx[12];

static void WriteAhead(const char *tuple, size_t len, int tid) {
  BigPage *page = bigpages[tid];
  com::xmemcopy<136>::copy(page->data + (over_offset[tid]), tuple);
  com::xmemcopy<122>::copy(page->data + (over_offset[tid] + 136UL), tuple + 142UL);
  over_offset[tid] += 258UL;
}

static void Write(const char *tuple, size_t len, uint32_t tid) {
  uint32_t write_count = nomal_offset[tid];
  if (tid < BigPageCount) {
    BigPage *page = bigpages[tid];
    uint64_t offset = write_count * 258UL;
    com::xmemcopy<136>::copy(page->data + (offset), tuple);
    com::xmemcopy<122>::copy(page->data + (offset + 136UL), tuple + 142UL);
    if(write_count < 10000) page -> offset = write_count + 1;
  } else {
    Page *page = pages[tid];
    uint64_t offset = (write_count % 64) * 258UL;
    com::xmemcopy<136>::copy(page->data + (offset), tuple);
    com::xmemcopy<122>::copy(page->data + (offset + 136UL), tuple + 142UL);
    if(write_count < 10000) page -> offset = write_count + 1;
    if ((write_count + 1) % 64 == 0) {
      // pthread_mutex_lock(&mem_mtx[tid % 6]);
      // pmem_memcpy(pmemaddress[tid] + PageData * page -> is_drty, page -> data, PageData, PMEM_F_MEM_NODRAIN|PMEM_F_MEM_NONTEMPORAL|PMEM_F_MEM_WC);
      pmem_memcpy_nodrain(pmemaddress[tid] + (write_count - 63) * 258UL, page->data, PageData);
      // pmem_drain();
      // pthread_mutex_unlock(&mem_mtx[tid % 6]);
    }
  }
  nomal_offset[tid]++;
}

typedef Status (*RecoveryCallBack)(const char *tuple, size_t len, uint64_t recovery_count);

static uint64_t NvmBufferRecover(RecoveryCallBack func) {
  uint64_t recovery_sum = 0;

  for (uint32_t i = 0; i < BigPageCount; i++) {
    BigPage *page = bigpages[i];
    uint32_t need_recovery = page -> offset;
    if (need_recovery == 10000) {
      need_recovery = 1070000;
      page -> offset = 1070000;
    }
    for (uint32_t j = 0; j < need_recovery; j++) {
      User user;
      memcpy(&user.id, page->data + (j * 258UL), 8);
      memcpy(user.user_id, page->data + (j * 258UL) + 8, 128);
      memcpy(user.name, page->data + (j * 258UL) + 8, 6);
      memcpy(user.name + 6, page->data + (j * 258UL) + 136, 122);
      func((char *)&user, 264UL, recovery_sum++);
    }      
  }

  for (uint32_t i = BigPageCount; i < MemBlockCount; i++) {
    Page *smallPage = pages[i];
    if (smallPage->offset == 10000) {
      if (i < 46) smallPage->offset = 930000;
      else smallPage->offset = 1000000;
    }

    for (int j = 0; j < smallPage -> offset % 64; j++) {
      User user;
      memcpy(&user.id, smallPage->data + (j * 258UL), 8);
      memcpy(user.user_id, smallPage->data + (j * 258UL) + 8U, 128);
      memcpy(user.name, smallPage->data + (j * 258UL) + 8U, 6);
      memcpy(user.name + 6U, smallPage->data + (j * 258UL) + 136U, 122);
      func((char *)&user, 264UL, recovery_sum++);
    }

    uint32_t flushCount = smallPage -> offset / 64;
    if(smallPage -> offset % 64 == 0 && flushCount > 0) {
      flushCount -= 1;
      for (int j = 0; j < 64; j++) {
        User user;
        memcpy(&user.id, smallPage->data + (j * 258UL), 8);
        memcpy(user.user_id, smallPage->data + (j * 258UL) + 8U, 128);
        memcpy(user.name, smallPage->data + (j * 258UL) + 8U, 6);
        memcpy(user.name + 6U, smallPage->data + (j * 258UL) + 136U, 122);
        func((char*)&user, 264UL, recovery_sum++);
      }
      pmem_memcpy_nodrain(pmemaddress[i] + (smallPage -> offset - 64) * 258UL, smallPage->data, PageData);
    }
    char *pmemPage = pmemaddress[i];
    for (uint32_t j = 0; j < flushCount; j++) {
      for (uint32_t k = 0; k < 64; k++) {
          User user;
          memcpy(&user.id, pmemPage + (j * 64 + k) * 258UL, 8);
          memcpy(user.user_id, pmemPage + (j * 64 + k) * 258UL + 8U, 128);
          memcpy(user.name, pmemPage + (j * 64 + k) * 258UL + 8U, 6);
          memcpy(user.name + 6U, pmemPage + (j * 64 + k) * 258UL + 136U, 122);
          func((char *)&user, 264UL, recovery_sum++);
      }
    }
  }
  return recovery_sum;
}

static void ReleaseMemory() {
}
