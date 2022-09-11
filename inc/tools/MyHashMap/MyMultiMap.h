#include <iostream>
#include <unordered_map>
#include <vector>
#include <cstdio>
#include "../HashMap/EMHash/emhash7_int64_to_int32.h"
#include "MyHashMap.h"

// todo(wq): release memory
// MyMultiMap目前仅仅提供insert和find功能
// 既不会释放内存，也不提供erase操作
template <typename KeyT, typename ValueT>
class MyMultiMap {
public:
    struct ValueWrapper;
    typedef typename std::pair<KeyT, ValueT> value_type;
    // typedef typename emhash7::HashMap<KeyT, ValueWrapper> Internal_HashMap; // 更换hashmap实现，修改这里即可
    typedef MyHashMap<KeyT, ValueWrapper> Internal_HashMap; // 更换hashmap实现，修改这里即可
    typedef typename Internal_HashMap::iterator Internal_iterator;
    typedef typename std::pair<KeyT, ValueWrapper> internal_value_type;

    // typedef value_type* pointer; // *和->运算符和internal的内部数据组织结构强耦合，太难做兼容了。。
    // typedef value_type& reference; // *和->运算符和internal的内部数据组织结构强耦合，太难做兼容了。。

    // 实际存储在Internal_HashMap中的value
    struct ValueWrapper {
        ValueWrapper(): vector_ptr_(nullptr) {}
        ValueWrapper(ValueT &&value): value_(std::move(value)), vector_ptr_(nullptr) {}
        ValueT value_;
        std::vector<ValueT> *vector_ptr_;
    };

    class iterator {
    public:
        iterator(Internal_iterator iter, size_t no): iter_(iter), no_(no) {}
        iterator(Internal_iterator &other_iter): iter_(other_iter.iter_), no_(other_iter.no_) {}
        
        KeyT& First() const { return iter_->first; }

        ValueT& Second() const {
            if (no_ == 0) {
                return iter_->second.value_;
            }
            return iter_->second.vector_ptr_->at(no_ - 1);
        }
        // todo(wq): *和->运算符和internal的内部数据结构强耦合，太难做兼容了。。
        // reference operator*() const {}

        // todo(wq): *和->运算符和internal的内部数据结构强耦合，太难做兼容了。。
        // pointer operator->() const {}

        bool operator==(const iterator& rhs) const { return iter_ == rhs.iter_ && no_ == rhs.no_; }
        bool operator!=(const iterator& rhs) const { return iter_ != rhs.iter_ || no_ != rhs.no_; }

        // ++i
        iterator& operator++() {
            if (no_ == 0) {
                if (iter_->second.vector_ptr_ != nullptr) {
                    no_++;
                } else {
                    iter_++;
                }
            } else {
                if (no_ == iter_->second.vector_ptr_->size()) {
                    iter_++;
                    no_ = 0;
                } else {
                    no_++;
                }
            }
            return *this;
        }

        // i++
        iterator operator++(int) {
            iterator old = *this;
            this->operator++();
            return old;
        }
    private:
        Internal_iterator iter_;
        size_t no_;
    };

    MyMultiMap(): size_(0) {};
    iterator begin() { return iterator(internal_.begin(), 0); }
    iterator end() { return iterator(internal_.end(), 0); }
    void reserve(uint64_t num_elems) { internal_.reserve(num_elems); }
    uint64_t size() const { return size_; }
    
    iterator insert(value_type &&kv_pair) {
        size_++;
        auto res = internal_.insert(internal_value_type(std::move(kv_pair.first), ValueWrapper(std::move(kv_pair.second))));
        if (res.second == true) {
            // 不存在重复元素
            return iterator(res.first, 0);
        }
        // 存在重复元素
        auto &value_wrapper = res.first->second;
        if (value_wrapper.vector_ptr_ == nullptr) {
            value_wrapper.vector_ptr_ = new std::vector<ValueT>();
        }
        value_wrapper.vector_ptr_->emplace_back(std::move(kv_pair.second));
        return iterator(res.first, value_wrapper.vector_ptr_->size());
    }

    std::pair<iterator, iterator> equal_range(const KeyT& key) {
        auto internal_iter = internal_.find(key);
        if (internal_iter == internal_.end()) {
            return {end(), end()};
        }
        auto range_start_iter = internal_iter;
        auto range_end_iter = internal_iter;
        range_end_iter++;
        return {iterator(range_start_iter, 0), iterator(range_end_iter, 0)};
    }

private:
    Internal_HashMap internal_;
    uint64_t size_;
};
