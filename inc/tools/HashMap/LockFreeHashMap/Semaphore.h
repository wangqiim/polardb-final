#include <assert.h>
#include <mutex>
#include <condition_variable>
#include <atomic>

namespace semaphore {
	class CountingSemaphore {
	private:
		std::mutex mtx;
		std::condition_variable cnd;
		std::atomic_uint count;
	public:
		std::atomic_uint active;

		CountingSemaphore(unsigned count = 0) :
			count(count), active(0) {}

		void acquire() {
			std::unique_lock<decltype(mtx)> lock(mtx);
			while (!count)
				cnd.wait(lock);
			--count;
			++active;
		}

		void release() {
			std::lock_guard<decltype(mtx)> lock(mtx);
			++count;
			--active;
			cnd.notify_one();
		}
	};
};