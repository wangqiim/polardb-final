#ifndef HRH_DB_H
#define HRH_DB_H

#include <stdint.h>
#include <stdio.h>

typedef int Status;

struct User {
    uint64_t id;
    char user_id[128];
    unsigned char name[128];
};

struct UserStress {
    uint64_t id;
    char user_id[128];
    char name[120];
};


enum Column{Id=0,Userid,Name,Salary};

class DB {
  public:
  DB() = default;

  virtual ~DB();  

  //tuple结构
  virtual Status Put(const char *tuple, size_t len) = 0;

  virtual Status Get(int32_t select_column,
            int32_t where_column, const void *column_key, size_t column_key_len, void *res) = 0;
};

#endif /* HRH_KVSTORE_H */