#pragma once

#ifndef HRH_HRHCONST_H
#define HRH_HRHCONSTE_H

//原始record的长度
const uint64_t RECORD_SIZE = 272;

const uint64_t PMEM_RECORD_SIZE = 256;
const uint64_t MEM_RECORD_SIZE = 16;

//表空间大小
const uint64_t TABLE_SIZE = 50000005;
const uint64_t TOTAL_WRITE_NUM = 1 << 28; // 复赛单节点总写入2亿数据

const uint64_t COMMIT_FLAG_SIZE = 4;
const uint64_t PER_THREAD_MAX_WRITE = 5e6; // 有的线程会写超过400W，这里取500万
//哈希索引分块
const uint64_t UK_HASH_MAP_SHARD = 1<<16;
const uint64_t SK_HASH_MAP_SHARD = 1<<16;
//NMV文件数
const uint64_t PMEM_FILE_COUNT = 50;

//有3个peer
const int PeerHostInfoNum = 3;

const int SYNC_TID = 100;

bool is_use_remote_pk = false;

enum Column{Id=0,Userid,Name,Salary};

#endif