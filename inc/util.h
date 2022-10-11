#include <vector>
#include <string>
#include <sys/stat.h>
#include <sys/mman.h>
#include <fstream>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include "spdlog/spdlog.h"
#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

// https://blog.csdn.net/qq_36849711/article/details/117997027
typedef struct cpu_occupy_          //定义一个cpu occupy的结构体
{
    char name[20];                  //定义一个char类型的数组名name有20个元素
    unsigned int user;              //定义一个无符号的int类型的user
    unsigned int nice;              //定义一个无符号的int类型的nice
    unsigned int system;            //定义一个无符号的int类型的system
    unsigned int idle;              //定义一个无符号的int类型的idle
    unsigned int iowait;
    unsigned int irq;
    unsigned int softirq;
}cpu_occupy_t;
 
class Util {
  public:
    static void print_resident_set_size() {
      double resident_set = 0.0;
      std::ifstream stat_stream("/proc/self/stat",std::ios_base::in);
      std::string pid, comm, state, ppid, pgrp, session, tty_nr;
      std::string tpgid, flags, minflt, cminflt, majflt, cmajflt;
      std::string utime, stime, cutime, cstime, priority, nice;
      std::string O, itrealvalue, starttime;
      unsigned long vsize;
      long rss;
      stat_stream >> pid >> comm >> state >> ppid >> pgrp >> session >> tty_nr
              >> tpgid >> flags >> minflt >> cminflt >> majflt >> cmajflt
              >> utime >> stime >> cutime >> cstime >> priority >> nice
              >> O >> itrealvalue >> starttime >> vsize >> rss;
      stat_stream.close();
      long page_size_kb = sysconf(_SC_PAGE_SIZE) / 1024;
      resident_set = (double)rss * (double)page_size_kb / (1024 * 1024);
      spdlog::info("plate current process memory size: {0:f}g", resident_set);
    }

    static void print_sysCpuUsage() {
      cpu_occupy_t cpu_stat1;
      cpu_occupy_t cpu_stat2;
      double cpu;
      get_cpuoccupy((cpu_occupy_t *)&cpu_stat1);
      sleep(1);
      //第二次获取cpu使用情况
      get_cpuoccupy((cpu_occupy_t *)&cpu_stat2);
  
      //计算cpu使用率
      cpu = cal_cpuoccupy((cpu_occupy_t *)&cpu_stat1, (cpu_occupy_t *)&cpu_stat2);
      spdlog::info("CPU usage: {}", cpu);
    }

  private:
    static double cal_cpuoccupy (cpu_occupy_t *o, cpu_occupy_t *n) {
      double od, nd;
      double id, sd;
      double cpu_use ;
  
      od = (double) (o->user + o->nice + o->system +o->idle+o->softirq+o->iowait+o->irq);//第一次(用户+优先级+系统+空闲)的时间再赋给od
      nd = (double) (n->user + n->nice + n->system +n->idle+n->softirq+n->iowait+n->irq);//第二次(用户+优先级+系统+空闲)的时间再赋给od
  
      id = (double) (n->idle);    //用户第一次和第二次的时间之差再赋给id
      sd = (double) (o->idle) ;    //系统第一次和第二次的时间之差再赋给sd
      if((nd-od) != 0)
        cpu_use =100.0 - ((id-sd))/(nd-od)*100.00; //((用户+系统)乖100)除(第一次和第二次的时间差)再赋给g_cpu_used
      else 
        cpu_use = 0;
      return cpu_use;
    }
    static void get_cpuoccupy(cpu_occupy_t *cpust) {
      FILE *fd;
      char buff[256];
      cpu_occupy_t *cpu_occupy;
      cpu_occupy=cpust;
  
      fd = fopen ("/proc/stat", "r");
      if(fd == NULL) {
        perror("fopen:");
        exit (0);
      }
      fgets (buff, sizeof(buff), fd);
  
      sscanf (buff, "%s %u %u %u %u %u %u %u", cpu_occupy->name, &cpu_occupy->user, &cpu_occupy->nice,&cpu_occupy->system, &cpu_occupy->idle ,&cpu_occupy->iowait,&cpu_occupy->irq,&cpu_occupy->softirq);
  
      fclose(fd);
    }
};
