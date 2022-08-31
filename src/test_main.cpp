#include "../inc/interface.h"
#include <iostream>
#include <string.h>
#include <pthread.h>
#include <thread>
#include <fstream>
#include <sys/time.h>
#include <atomic>

class TestUser
{
public:
    uint64_t id;
    unsigned char user_id[128];
    unsigned char name[128];
    uint64_t salary;
};

enum TestColumn{Id=0,Userid,Name,Salary};

#define NUM_THREADS 50

std::string randstr(int max_len)
{
    // return std::string("V84H3fu651p366vxeUG7NDO74oscL45ZXLVMRTaO321aOrGYj7VQK6tRpqpx3YGiH9n1ande452tv6p83Bkkm3405Nw96WLO36M14qiIpRHgr76KFK92RM47V1191Sd");

    int real_len = max_len;
    if(real_len == 0) return "";
    char* str = (char*)malloc(real_len);
    
    for (int i = 0; i < real_len; ++i)
    {
        switch ((rand() % 3))
        {
        case 1:
            str[i] =  1 + rand() % 26;
            break;
        case 2:
            str[i] =  1 + rand() % 80;
            break;
        default:
            str[i] =  1 + rand() % 110;
            break;
        }
    }
    str[real_len -1] = '\0';
    std::string string = str;
    free(str);
    return string;
}

// 线程的运行函数
void* thread_write(void* ctx)
{   
    int id = *(int *)ctx;
    std::string file = "./Dataset/user" + std::to_string(id) + ".dat";
    // std::cout << file << std::endl;
    std::ifstream inFile(file,std::ios::in| std::ios::binary);
    if(!inFile) {
        std::cout << "error" << std::endl;
        return 0;
    }
    TestUser user;
    while(inFile.read((char *)&user, sizeof(user))) { //一直读到文件结束
        engine_write(ctx,&user,sizeof(user));
    }
    inFile.close();
    return 0;
}

void* thread_read(void* ctx)
{
    TestUser user;
    char res[8000*128];
    int id = *(int *)ctx;
    for(int i = 0; i < 1000000; i++) {
        char res[2000*128];
        user.id = id * 1000000UL + i;
        user.salary = id * 10000000UL + 100 + i%20000 * 10;
        memcpy(&user.name,"hrh",3); 
        memcpy(&user.user_id,"1000085000",11); 
        engine_read(ctx, Salary, Id, &user.id, 8, res);
        engine_read(ctx, Id, Salary, &user.salary, 8, res);
        engine_read(ctx, Name, Id, &user.id, 8, res);
    }
    return 0;
}

bool test_is_ok(void *ctx) {
    TestUser user;
    char res[2000*128];
    user.id = 17000010;
    user.salary = 170000200;
    memcpy(&user.name,"hrh\n0000\nqwer",14); 
    int8_t user_id_int[128] = {4,13,13,5,22,35,18,6,82,59,22,21,42,19,11,71,72,51,69,22,94,104,22,39,21,24,43,5,55,21,110,20,71,10,89,1,60,47,7,68,38,55,74,96,71,35,5,63,36,4,12,30,22,7,21,49,15,6,25,8,8,5,7,79,61,87,92,75,6,25,17,23,20,57,20,17,13,38,26,3,44,29,23,59,16,9,51,67,3,36,10,71,93,12,38,25,15,13,71,34,22,60,9,82,54,72,109,20,54,1,47,5,27,42,6,14,33,23,22,81,12,23,19,34,17,25,17,0};
    for(int i = 0; i < 128; i++) user.user_id[i] = user_id_int[i];
    int record_count = 0;
    record_count = engine_read(ctx, Salary, Id, &user.id, 8, res);
    std::cout << "查询 Salary = " << *(uint64_t *)(res) << " where id = " <<  user.id << " Count = " << record_count << std::endl;
    // if(record_count != 1) {
    //     return false;
    // }
    record_count = engine_read(ctx, Id, Salary, &user.salary, 8, res);
    for(int i = 0; i<50; i++)
    std::cout << "查询 id = " << *(int64_t *)(res + i * 8) << " where salary = " << user.salary << " Count = " << record_count << std::endl;
    // if(record_count != 1) {
    //     return false;
    // }

    // record_count = engine_read(ctx, Userid, Name, &user.name, 128, res);
    // char user_id[128];
    // memcpy(user_id, res, 128);
    // std::cout << "查询 Userid = " << user_id << " where name = " << user.name << " Count = " << record_count << std::endl;
    // if(record_count != 1) {
    //     return false;
    // }
    record_count = engine_read(ctx, Name, Userid, user.user_id, 128, res);
    char name[128];
    memcpy(name, res, 128);
    std::cout << "查询 name = ";
    for(int i = 0; i < 128; i++) printf("%u-",name[i]);
    std::cout << std::endl;
    std::cout << "where user_id = ";
    for(int i = 0; i < 128; i++) printf("%u-",user.user_id[i]);
    std::cout << std::endl;
    std::cout << "Count = " << record_count << std::endl;
    // if(record_count != 1) {
    //     return false;
    // }

    record_count = engine_read(ctx, Userid, Id, &user.id, 8, res);
    char userid[128];
    memcpy(userid, res, 128);
    std::cout << "查询 userid = " << userid << " where id = " << user.id << " Count = " << record_count << std::endl;
    // user.id = 1;
    // user.salary = 100;
    // memcpy(&user.name,"hellow",6); 
    // memcpy(&user.user_id,"1122333",7); 
    // engine_write(ctx,&user,sizeof(user));    
    // if(engine_read(ctx, Id, Id, &user.id, 8, res) != 1) return false;
    // if(engine_read(ctx, Salary, Salary, &user.salary, 8, res) != 2) return false;
    // if(engine_read(ctx, Name, Name, &user.name, 128, res) != 1) return false;
    // if(engine_read(ctx, Userid, Userid, &user.user_id, 128, res) != 1) return false;   
    return true; 
}

void test_write(void *ctx) {
    pthread_t tids[NUM_THREADS];
    for(int i = 0; i < NUM_THREADS; ++i)
    {
        int fid = i;
        //参数依次是：创建的线程id，线程参数，调用的函数，传入的函数参数
        int ret = pthread_create(&tids[i], NULL, thread_write, &fid);
        if (ret != 0)
        {
           std::cout << "pthread_create error: error_code=" << ret << std::endl;
        }
    }
    pthread_exit(NULL);
}

void test_read(void *ctx) {
    pthread_t tids[NUM_THREADS];
    for(int i = 0; i < NUM_THREADS; ++i)
    {
        int fid = i;
        //参数依次是：创建的线程id，线程参数，调用的函数，传入的函数参数
        int ret = pthread_create(&tids[i], NULL, thread_read, &fid);
        if (ret != 0)
        {
           std::cout << "pthread_create error: error_code=" << ret << std::endl;
        }
    }
    pthread_exit(NULL);
}

static std::atomic<uint8_t> threadId(0);
void write_400M (void* ctx) {
    uint8_t id = threadId++;
    std::string file = "./Dataset/user" + std::to_string(id) + ".dat";
    std::ofstream outFile(file, std::ios::out | std::ios::binary);
    for(unsigned long i = 0; i < 4000000UL; i++) {
        TestUser user;
        user.id = id * 4000000UL + i;
        user.salary = id;
        memcpy(user.name,randstr(128).c_str(),128);
        memcpy(user.user_id,randstr(128).c_str(),128); 
        outFile.write((char *)&user, sizeof(user));
    }
    outFile.close();
}

int main()
{
    char *myIp = "127.0.0.1:9000";
    char *server[2] = {"127.0.0.2:8080","127.0.0.3:8080"};
    void* ctx = engine_init(myIp, server, 2, "/mnt/pmem0/pmemData", "./");
    struct timeval t1,t2;
    double timeuse;
    gettimeofday(&t1,NULL);
    // test_write(ctx);
    // engine_deinit(nullptr);
    // test_read(ctx);

    // for(int i = 0; i < 50; ++i)
    // {   
    //     write_400M(nullptr);
    // }

    if(test_is_ok(ctx)) {
        std::cout << "正确性验证成功！" << std::endl;
    }

    gettimeofday(&t2,NULL);
    timeuse = (t2.tv_sec - t1.tv_sec) * 1000 + (double)(t2.tv_usec - t1.tv_usec)/1000.0;
    std::cout<<"time = "<< timeuse << "ms" << std::endl;  //输出时间（单位：ｓ）

}