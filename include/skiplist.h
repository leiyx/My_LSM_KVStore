#ifndef LSM_KV_SKIPLIST_H
#define LSM_KV_SKIPLIST_H

#include <bitset>
#include <cmath>
#include <fstream>
#include <iostream>
#include <string>
#include <vector>

#include "Murmur_hash3.h"
#include "kvstore_api.h"
#include "options.h"

struct Node {
  Node *right_, *down_;
  int64_t key_;
  std::string val_;
  Node(Node *right, Node *down, int64_t key, const std::string &val)
      : right_(right), down_(down), key_(key), val_(val) {}
  Node() : right_(nullptr), down_(nullptr), key_(INT64_MIN), val_("") {}
};

class SkipList {
 public:
  // 转换成SSTable占用的空间大小
  size_t memory_;

 public:
  SkipList()
      : size_(0),
        head_(nullptr),
        memory_(options::kInitialSize),
        time_stamp_(0),
        min_key_(INT64_MAX),
        max_key_(INT64_MIN) {}

  ~SkipList();

  /**
   * @brief 查找键值对
   * @param[in] key 查找键值对的键值
   * @return string 返回key对应的val，如果key不存在则返回""
   */
  std::string Get(int64_t key) const;

  /**
   * @brief 插入键值对
   * @param[in] key 键
   * @param[in] val 值
   */
  void Put(int64_t key, const std::string &val);

  /**
   * @brief 将MemTable储存为L0层SSTable, Minor MinorCompaction
   * @param[in] num
   * @param[in] dir
   */
  void Store(int num, const std::string &dir);

  Node *GetFirstNode() const;

  size_t GetSize() const { return size_; }

 private:
  uint64_t size_;        // 储存的键值对个数
  Node *head_;           // 头结点
  uint64_t time_stamp_;  // 时间戳，即SSTable序号
  int64_t min_key_;
  int64_t max_key_;
};

#endif  // LSM_KV_SKIPLIST_H
