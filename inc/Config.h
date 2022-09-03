#pragma once

#ifndef HRH_HRHCONST_H
#define HRH_HRHCONSTE_H

//原始record的长度
const uint64_t RECORD_SIZE = 272;

//表空间大小
const uint64_t TABLE_SIZE = 50000005;

// 索引初始容量
const uint64_t INDEX_CAPACITY = 2e8;

const uint64_t COMMIT_FLAG_SIZE = 4;
const uint64_t PER_THREAD_MAX_WRITE = 5e6; // 有的线程会写超过400W，这里取500万
//哈希索引分块
const uint64_t HASH_MAP_COUNT = 50;
//NMV文件数
const uint64_t PMEM_FILE_COUNT = 50;

//有3个peer
const int PeerHostInfoNum = 3;

// rpc timeout 默认所有的调用的在60s内都能成功，不成功则打印error log
const int64_t RPC_TIMEOUT = 60000;

enum Column{Id=0,Userid,Name,Salary};

#endif