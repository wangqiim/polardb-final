#pragma once

#ifndef HRH_HRHCONST_H
#define HRH_HRHCONSTE_H

//原始record的长度
const uint64_t RECORD_SIZE = 272;

const uint64_t PMEM_RECORD_SIZE = 256;
const uint64_t MEM_RECORD_SIZE = 12;

//表空间大小
const uint64_t TABLE_SIZE = 50000005;
const uint64_t TOTAL_WRITE_NUM = 2e8; // 复赛单节点总写入2亿数据

const uint64_t COMMIT_FLAG_SIZE = 4;
const uint64_t PER_THREAD_MAX_WRITE = 4000000; // 每个线程必须写400W，勿修改
//哈希索引分块
const uint64_t UK_HASH_MAP_SHARD = 1<<17;
const uint64_t SK_HASH_MAP_SHARD = 1<<17;
//NMV文件数
const uint64_t PMEM_FILE_COUNT = 50;

//有3个peer
const int PeerHostInfoNum = 3;

const int MAX_Client_Num = 100; // 最大允许的线程数，初始化时，只初始化前50个读写client，之后的懒初始化
const int SYNC_Init_Deinit_Tid = 0;

const uint32_t Salary_Cache_Num = 6e8;

bool is_use_remote_pk = false;

uint64_t recovery_cnt = 0;

uint32_t salary_page_cnt = 6;

enum Column{Id=0,Userid,Name,Salary};

// NvmStore.h
static char *GetUserIdByPos(uint32_t pos);
 
//#define debug_db

#endif