#ifndef HRH_SPIN_MUTEX_H
#define HRH_SPIN_MUTEX_H
#include <atomic>

//利用atomic变量简单实现一个自旋锁
class spin_mutex {
    // flag对象所封装的bool值为false时，说明自旋锁未被线程占有。  
    std::atomic<bool> flag = ATOMIC_VAR_INIT(false);       
public:
    spin_mutex() = default;
    // spin_mutex(const spin_mutex&) = delete;
    spin_mutex& operator= (const spin_mutex&) = delete;
    void lock() {
        bool expected = false;
        // CAS原子操作。判断flag对象封装的bool值是否为期望值(false)，若为bool值为false，与期望值相等，说明自旋锁空闲。
        // 此时，flag对象封装的bool值写入true，CAS操作成功，结束循环，即上锁成功。
        // 若bool值为为true，与期望值不相等，说明自旋锁被其它线程占据，即CAS操作不成功。然后，由于while循环一直重试，直到CAS操作成功为止。
        while(!flag.compare_exchange_strong(expected, true)){ 
            expected = false;
        }      
    }
    void unlock() {
        flag.store(false);
    }
};

#endif