#pragma once
#include <iostream>
#include <vector>

// 
// !!!!!!!! 使用之前必须先reserve !!!!!!!!!!!!
// MyHashMapV2目前仅仅提供insert和find功能
// 不提供扩容操作，因此需要提前选择合适的值，进行reserve

/**
 * 对于每个桶的数据组织结构如下
 * 相比于v1，占用更少的内存（缓解了v1版本的pair<key, value>内存对齐造成的内存浪费）
 * ------------------------------------------------------------------------------------------
 * | key_type | key_type | key_type | val_type | val_type | val_type | vector<value_type> * |
 * ------------------------------------------------------------------------------------------
 * |    key   |    key   |    key   |   value  |   value  |   value  |        ptr           |
 * ------------------------------------------------------------------------------------------
 * 
 */
const uint64_t V2InplaceValueNum = 1; // 存储在原地的值的个数

template <typename KeyT, typename ValueT, typename HashT = std::hash<KeyT>, typename EqT = std::equal_to<KeyT>>
class MyHashMapV2 {
public:
  typedef typename std::pair<KeyT, ValueT> value_type;
  typedef value_type* pointer;
  typedef value_type& reference;
  
  struct bucket_type {
    KeyT keys[V2InplaceValueNum];
    ValueT values[V2InplaceValueNum];
    // value_type inplace_value[V2InplaceValueNum];
    std::vector<value_type> *ptr = nullptr;
  };

  class iterator {
    public:
      iterator(uint8_t *value_cnts, bucket_type *buckets, uint64_t cur_idx, uint64_t end_idx, uint64_t value_no)
        : value_cnts_(value_cnts), buckets_(buckets), cur_idx_(cur_idx), end_idx_(end_idx), value_no_(value_no) {}

      // reference operator*() const {
      //   if (value_no_ < V2InplaceValueNum) {
      //       return buckets_[cur_idx_].inplace_value[value_no_];
      //   }
      //   return buckets_[cur_idx_].ptr->at(value_no_ - V2InplaceValueNum);
      // }
      // pointer operator->() const {
      //   if (value_no_ < V2InplaceValueNum) {
      //       return &(buckets_[cur_idx_].inplace_value[value_no_]);
      //   }
      //   return &(buckets_[cur_idx_].ptr->at(value_no_ - V2InplaceValueNum));
      // }
      KeyT& First() const {
        if (value_no_ < V2InplaceValueNum) {
          return buckets_[cur_idx_].keys[value_no_];
        }
        return buckets_[cur_idx_].ptr->at(value_no_ - V2InplaceValueNum).first;
      }

      ValueT& Second() const {
        if (value_no_ < V2InplaceValueNum) {
          return buckets_[cur_idx_].values[value_no_];
        }
        return buckets_[cur_idx_].ptr->at(value_no_ - V2InplaceValueNum).second;
      }

      // 将两个容器的迭代器进行比较是ub的
      bool operator==(const iterator& rhs) const { return cur_idx_ == rhs.cur_idx_ && value_no_ == rhs.value_no_; }
      bool operator!=(const iterator& rhs) const { return cur_idx_ != rhs.cur_idx_ || value_no_ != rhs.value_no_; }

      // ++i 注意，如果当前iter已经是end()，则++不会产生任何影响
      iterator& operator++() {
        if (value_no_ + 1 < value_cnts_[cur_idx_]) {
            value_no_++;
        } else {
          cur_idx_++;
          value_no_ = 0;
          while (cur_idx_ != end_idx_ && value_cnts_[cur_idx_] == 0) {
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
      uint8_t *value_cnts_;
      bucket_type *buckets_;
      uint64_t cur_idx_;
      uint64_t end_idx_;
      uint64_t value_no_;
  };

  MyHashMapV2(): value_cnts_(nullptr), buckets_(nullptr), bucket_cnt_(0)
    , hasher_(), equaler_(), size_(0) {}
  void reserve(uint64_t num_elems) { 
    if (buckets_ != nullptr || bucket_cnt_ != 0) {
        std::cerr << "reserve fail!" << std::endl;
        exit(1);
    }
    bucket_cnt_ = 1;
    while (bucket_cnt_ < num_elems) {
        bucket_cnt_ <<= 1;
    }
    bucket_cnt_ = num_elems * 1.2;
    value_cnts_ = new uint8_t[bucket_cnt_];
    buckets_ = new bucket_type[bucket_cnt_];
  }

  iterator begin() {
    uint64_t bucket_idx = 0;
    while (bucket_idx < bucket_cnt_ && value_cnts_[bucket_idx] == 0) {
      bucket_idx++;
    }
    return iterator(value_cnts_, buckets_, bucket_idx, bucket_cnt_,  0);
  }
  iterator end() { return iterator(value_cnts_, buckets_, bucket_cnt_, bucket_cnt_, 0); }

  std::pair<iterator, bool> insert(value_type &&kv_pair) {
    uint64_t bucket_id = hasher_(kv_pair.first) % bucket_cnt_;
    // 1. 如果该值已经存在直接返回.
    for (uint64_t i = 0; i < value_cnts_[bucket_id]; i++) {
      auto iter = iterator(value_cnts_, buckets_, bucket_id, bucket_cnt_, i);
      if (equaler_(iter.First(), kv_pair.first)) {
        return {iter, false};
      }
    }
    size_++;
    if (value_cnts_[bucket_id]  < V2InplaceValueNum) { // 原地还有空
      auto write_idx = value_cnts_[bucket_id]++;
      buckets_[bucket_id].keys[write_idx] = std::move(kv_pair.first);
      buckets_[bucket_id].values[write_idx] = std::move(kv_pair.second);
      return {iterator(value_cnts_, buckets_, bucket_id, bucket_cnt_, write_idx), true};
    }
    if (value_cnts_[bucket_id] == V2InplaceValueNum) { // 原地没空
      buckets_[bucket_id].ptr = new std::vector<value_type>();
    }
    auto write_idx = value_cnts_[bucket_id]++;
    buckets_[bucket_id].ptr->emplace_back(std::move(kv_pair));
    return {iterator(value_cnts_, buckets_, bucket_id, bucket_cnt_,  write_idx), true};
  }

  iterator find(const KeyT& key) {
    uint64_t bucket_id = hasher_(key) % bucket_cnt_;
    for (uint64_t no = 0; no < value_cnts_[bucket_id]; no++) {
      if (no < V2InplaceValueNum) { // 原地
        if (equaler_(buckets_[bucket_id].keys[no], key)) {
          return iterator(value_cnts_, buckets_, bucket_id, bucket_cnt_, no);
        }
      } else { // 非原地
        if (equaler_(buckets_[bucket_id].ptr->at(no - V2InplaceValueNum).first, key)) {
            return iterator(value_cnts_, buckets_, bucket_id, bucket_cnt_, no);
        }
      }
    }
    return end();
  }
  
  uint64_t size() const { return size_; }
private:

  uint8_t *value_cnts_; //该桶内目前有几个元素
  bucket_type *buckets_;
  uint64_t bucket_cnt_;
  HashT hasher_;
  EqT equaler_;
  uint64_t size_;
};
