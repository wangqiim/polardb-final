#pragma once
#include <iostream>
#include <vector>

// !!!!!!!! 使用之前必须先reserve !!!!!!!!!!!!
// MyHashMap目前仅仅提供insert和find功能
// 不提供扩容操作，因此需要提前选择合适的值，进行reserve

/**
 * 对于每个桶的数据组织结构
 * ------------------------------------------------------------------------------
 * | value cnt | value_type | value_type | value_type | vector<value_type> *
 * ------------------------------------------------------------------------------
 * | uint64_t  | pair<k,v>  | pair<k,v>  | pair<k,v>  |         ptr
 * ------------------------------------------------------------------------------
 * 
 */
const uint64_t InplaceValueNum = 3; // 存储在原地的值

template <typename KeyT, typename ValueT, typename HashT = std::hash<KeyT>, typename EqT = std::equal_to<KeyT>>
class MyHashMap {
public:
  typedef typename std::pair<KeyT, ValueT> value_type;
  typedef value_type* pointer;
  typedef value_type& reference;
  
  struct bucket_type {
    uint64_t value_cnt = 0; //该桶内目前有几个元素
    value_type inplace_value[InplaceValueNum];
    std::vector<value_type> *ptr = nullptr;
  };

  class iterator {
    public:
      iterator(bucket_type *buckets, uint64_t cur_idx, uint64_t end_idx, uint64_t value_no)
        : buckets_(buckets), cur_idx_(cur_idx), end_idx_(end_idx), value_no_(value_no) {}

      reference operator*() const {
        if (value_no_ < InplaceValueNum) {
            return buckets_[cur_idx_].inplace_value[value_no_];
        }
        return buckets_[cur_idx_].ptr->at(value_no_ - InplaceValueNum);
      }
      pointer operator->() const {
        if (value_no_ < InplaceValueNum) {
            return &(buckets_[cur_idx_].inplace_value[value_no_]);
        }
        return &(buckets_[cur_idx_].ptr->at(value_no_ - InplaceValueNum));
      }

      // 将两个容器的迭代器进行比较是ub的
      bool operator==(const iterator& rhs) const { return cur_idx_ == rhs.cur_idx_ && value_no_ == rhs.value_no_; }
      bool operator!=(const iterator& rhs) const { return cur_idx_ != rhs.cur_idx_ || value_no_ != rhs.value_no_; }

      // ++i 注意，如果当前iter已经是end()，则++不会产生任何影响
      iterator& operator++() {
        if (value_no_ + 1 < buckets_[cur_idx_].value_cnt) {
            value_no_++;
        } else {
          cur_idx_++;
          value_no_ = 0;
          while (cur_idx_ != end_idx_ && buckets_[cur_idx_].value_cnt == 0) {
            cur_idx_++;
          }
        }
        return *this;
      }
      // i++ 注意，如果当前iter已经是end()，则++不会产生任何影响
      iterator operator++(int) {
        iterator old = *this;
        this->operator++();
        return old;
      }
    private:
      bucket_type *buckets_;
      uint64_t cur_idx_;
      uint64_t end_idx_;
      uint64_t value_no_;
  };

  MyHashMap(): buckets_(nullptr), bucket_cnt_(0)
    , hasher_(), equaler_(), size_(0) {}
  void reserve(uint64_t num_elems) { 
    if (buckets_ != nullptr || bucket_cnt_ != 0) {
        std::cerr << "reserve fail!" << std::endl;
        exit(1);
    }
    // bucket_cnt_ = 1;
    // while (bucket_cnt_ < num_elems) {
    //     bucket_cnt_ <<= 1;
    // }
    bucket_cnt_ = num_elems + 1;
    buckets_ = new bucket_type[bucket_cnt_];
  }

  iterator begin() {
    uint64_t bucket_idx = 0;
    while (bucket_idx < bucket_cnt_ && buckets_[bucket_idx].value_cnt == 0) {
      bucket_idx++;
    }
    return iterator(buckets_, bucket_idx, bucket_cnt_,  0);
  }
  iterator end() { return iterator(buckets_, bucket_cnt_, bucket_cnt_, 0); }

  std::pair<iterator, bool> insert(value_type &&kv_pair) {
    uint64_t bucket_id = hasher_(kv_pair.first) % bucket_cnt_;
    for (uint64_t i = 0; i < buckets_[bucket_id].value_cnt; i++) {
      auto iter = iterator(buckets_, bucket_id, bucket_cnt_, i);
      if (equaler_(iter->first, kv_pair.first)) {
        return {iter, false};
      }
    }
    size_++;
    if (buckets_[bucket_id].value_cnt  < InplaceValueNum) { // 原地还有空
      auto write_idx = buckets_[bucket_id].value_cnt++;
      buckets_[bucket_id].inplace_value[write_idx] = std::move(kv_pair);
      return {iterator(buckets_, bucket_id, bucket_cnt_, write_idx), true};
    }
    if (buckets_[bucket_id].value_cnt == InplaceValueNum) { // 原地没空
      buckets_[bucket_id].ptr = new std::vector<value_type>();
    }
    auto write_idx = buckets_[bucket_id].value_cnt++;
    buckets_[bucket_id].ptr->emplace_back(std::move(kv_pair));
    return {iterator(buckets_, bucket_id, bucket_cnt_,  write_idx), true};
  }

  iterator find(const KeyT& key) {
    uint64_t bucket_id = hasher_(key) % bucket_cnt_;
    for (uint64_t no = 0; no < buckets_[bucket_id].value_cnt; no++) {
      if (no < InplaceValueNum) { // 原地
        if (equaler_(buckets_[bucket_id].inplace_value[no].first, key)) {
          return iterator(buckets_, bucket_id, bucket_cnt_, no);
        }
      } else { // 非原地
        if (equaler_(buckets_[bucket_id].ptr->at(no - InplaceValueNum).first, key)) {
            return iterator(buckets_, bucket_id, bucket_cnt_, no);
        }
      }
    }
    return end();
  }
  
  uint64_t size() const { return size_; }
private:
  bucket_type *buckets_;
  uint64_t bucket_cnt_;
  HashT hasher_;
  EqT equaler_;
  uint64_t size_;
};
