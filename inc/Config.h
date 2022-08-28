#pragma once

#ifndef HRH_HRHCONST_H
#define HRH_HRHCONSTE_H

//表空间大小
const int TABLE_SIZE = 50000005;

const int PER_THREAD_MAX_WRITE = 20000000;
//哈希索引分块
const int HASH_MAP_COUNT = 50;
//NMV文件数
const int PMEM_FILE_COUNT = 50;
//NVM文件空间
const unsigned long PMEM_SIZE = (1UL << 36);


const int WRITEMEM_THREAD = 16;
//NVM 分块数
const int NVMBLOCK_COUNT = 1;
//写NVM线程数
const int WRITENVM_THREAD = 2;
//读NVM线程数
const int READNVM_THREAD = 2;


enum Column{Id=0,Userid,Name,Salary};

#endif