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
};
