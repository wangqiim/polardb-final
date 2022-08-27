#include <atomic>
#include <assert.h>

using std::atomic;

// Markable reference with atomic operations
template<class T>
class MarkableReference
{
private:
    // Value and low bit mask
    atomic<uintptr_t> val;
    static const uintptr_t mask = 1;

    // Combine the reference and mark
    uintptr_t combine(T *ref, bool mark) {
        return reinterpret_cast<uintptr_t>(ref) | mark;
    }

public:
    // Constructor
    MarkableReference(T *ref = nullptr, bool mark = false)
        : val(combine(ref, mark)) {
        assert(
            (reinterpret_cast<uintptr_t>(ref) & mask) == 0 &&
            "low bit must be clear"
        );
    }

    MarkableReference(
        MarkableReference &other,
        std::memory_order order = std::memory_order_seq_cst
    ) : val(other.val.load(order)) {}

    // Override the = operator to do atomic sets
    MarkableReference &operator=(const MarkableReference &other) {
        val.store(
            other.val.load(std::memory_order_relaxed),
            std::memory_order_relaxed
        );
        return *this;
    }

    /*
     * GETTERS
     */

    T *getRef(std::memory_order order = std::memory_order_seq_cst) {
        return reinterpret_cast<T *>(val.load(order) & ~mask);
    }

    bool getMark(std::memory_order order = std::memory_order_seq_cst) {
        return (val.load(order) & mask);
    }

    T *getBoth(
        bool &mark,
        std::memory_order order = std::memory_order_seq_cst
    ) {
        uintptr_t current = val.load(order);
        mark = current & mask;
        return reinterpret_cast<T *>(current & ~mask);
    }

    /*
     * SET AND EXCHANGE
     */

    // Exchange the reference
    T *exchangeRef(
        T *ref,
        std::memory_order order = std::memory_order_seq_cst
    ) {
        uintptr_t old = val.load(std::memory_order_relaxed);
        bool success;
        do {
            uintptr_t newval = reinterpret_cast<uintptr_t>(ref) | (old & mask);
            success = val.compare_exchange_weak(old, newval, order);
        } while(!success);

        return reinterpret_cast<T *>(old & ~mask);
    }

    // Compare and exchange both items, update expected on failure
    bool compareExchangeBothWeak(
        T *&expectRef,
        bool &expectMark,
        T *desiredRef,
        bool desiredMark,
        std::memory_order order = std::memory_order_seq_cst
    ) {
        uintptr_t desired = combine(desiredRef, desiredMark);
        uintptr_t expected = combine(expectRef, expectMark);
        bool status = val.compare_exchange_weak(expected, desired, order);
        expectRef = reinterpret_cast<T *>(expected & ~mask);
        expectMark = expected & mask;
        return status;
    }

    // Set the mark
    void setMark(
        bool mark,
        std::memory_order order = std::memory_order_seq_cst
    ) {
        if (mark) val.fetch_or(mask, order);
        else val.fetch_and(~mask, order);
    }

    bool toggleMark(std::memory_order order = std::memory_order_seq_cst) {
        return mask & val.fetch_xor(mask, order);
    }

    // Exchange the mark
    bool exchangeMark(
        bool mark,
        std::memory_order order = std::memory_order_seq_cst
    ) {
        uintptr_t old;
        if (mark) old = val.fetch_or(mask, order);
        else old = val.fetch_and(~mask, order);

        return (old & mask);
    }
};
