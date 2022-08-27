#include <_types/_uint32_t.h>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include "./network/Group.h"
#include "./store/NvmStore.h"
#include "./index/MemIndex.h"

static void initNvmDB(const char* host_info, const char* const* peer_host_info, size_t peer_host_info_num,
                const char* aep_dir, const char* disk_dir){
    initGroup(host_info, peer_host_info, peer_host_info_num);
    initStore(aep_dir, peer_host_info_num);
}


static void Put(const char *tuple, size_t len){
    writeTuple(tuple, len);
    insert(tuple, len);
}

static size_t Get(int32_t select_column,
          int32_t where_column, const void *column_key, size_t column_key_len, void *res){
    std::vector<uint32_t> posArray = getPosFromKey(where_column, column_key);
    if (posArray.size() == 0) {

    } else {
      for (uint32_t pos : posArray) {
        readColumFromPos(select_column, pos, res);
      }
    }
    return 0;
}


static void deinitNvmDB() {

}