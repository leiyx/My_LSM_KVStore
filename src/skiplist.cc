#include "skiplist.h"

SkipList::~SkipList() {
  if (!head_) return;
  // 释放掉所有数据，防止内存泄漏
  Node *p = head_, *q = nullptr, *down = nullptr;
  do {
    down = p->down_;
    do {
      q = p->right_;
      delete p;
      p = q;
    } while (p);
    p = down;
  } while (p);

  // head指针置空，重置除时间戳外的其它数据项
  head_ = nullptr;
  size_ = 0;
  memory_ = 10272;
  min_key_ = INT64_MAX;
  max_key_ = INT64_MIN;
}

std::string SkipList::Get(int64_t key) const {
  if (size_ == 0) return "";
  Node *p = head_;
  while (p) {
    while (p->right_ && p->right_->key_ < key) {
      p = p->right_;
    }
    if (p->right_ && p->right_->key_ == key) return (p->right_->val_);
    p = p->down_;
  }
  return "";
}

void SkipList::Put(int64_t key, const std::string &val) {
  // 更新最大最小值键
  if (key < min_key_) min_key_ = key;
  if (key > max_key_) max_key_ = key;

  std::vector<Node *> path_list;  // 从上至下记录搜索路径
  Node *p = head_;
  while (p) {
    while (p->right_ && p->right_->key_ <= key) {
      p = p->right_;
    }
    path_list.push_back(p);
    p = p->down_;
  }

  // 对于相同的key，MemTable中进行替换
  if (!path_list.empty() && path_list.back()->key_ == key) {
    while (!path_list.empty() && path_list.back()->key_ == key) {
      Node *node = path_list.back();
      path_list.pop_back();
      node->val_ = val;
    }
    return;
  }

  // 如果不存在相同的key，则进行插入
  bool insertUp = true;
  Node *down_node = nullptr;
  size_++;
  while (insertUp && !path_list.empty()) {  // 从下至上搜索路径回溯，50%概率
    Node *insert = path_list.back();
    path_list.pop_back();

    insert->right_ =
        new Node(insert->right_, down_node, key, val);  // add新结点
    down_node = insert->right_;  // 把新结点赋值为downNode
    insertUp = (rand() & 1);     // 50%概率
  }
  if (insertUp) {  // 插入新的头结点，加层
    Node *oldHead = head_;
    head_ = new Node();
    head_->right_ = new Node(nullptr, down_node, key, val);
    head_->down_ = oldHead;
  }
}

void SkipList::Store(int num, const std::string &dir) {
  std::string file_name = dir + "/SSTable" + std::to_string(num) + ".sst";
  std::fstream out_file(file_name, std::ios::app | std::ios::binary);

  Node *node = GetFirstNode()->right_;
  time_stamp_++;  // ?

  // 写入时间戳、键值对个数和最小最大键
  out_file.write((char *)(&time_stamp_), sizeof(uint64_t));
  out_file.write((char *)(&size_), sizeof(uint64_t));
  out_file.write((char *)(&min_key_), sizeof(int64_t));
  out_file.write((char *)(&max_key_), sizeof(int64_t));

  // 写入生成对应的布隆过滤器
  std::bitset<81920> filter_;
  uint64_t temp_key;
  const char *temp_value;
  unsigned int hash[4] = {0};
  while (node) {
    temp_key = node->key_;
    MurmurHash3_x64_128(&temp_key, sizeof(temp_key), 1, hash);
    for (auto i : hash) filter_.set(i % 81920);
    node = node->right_;
  }
  out_file.write((char *)(&filter_), sizeof(filter_));

  // 索引区，计算key对应的索引值
  const int data_area = 10272 + size_ * 12;  // 数据区开始的位置
  uint32_t index = 0;
  node = GetFirstNode()->right_;
  int length = 0;
  int i = 0;
  while (node) {
    i++;
    temp_key = node->key_;
    index = data_area + length;
    out_file.write((char *)(&temp_key), sizeof(int64_t));
    out_file.write((char *)(&index), sizeof(uint32_t));
    length += node->val_.size() + 1;
    node = node->right_;
  }

  // 数据区，存放value
  node = GetFirstNode()->right_;
  while (node) {
    temp_value = (node->val_).c_str();
    out_file.write(temp_value, sizeof(char) * (node->val_.size()));
    temp_value = "\0";
    out_file.write(temp_value, sizeof(char) * 1);
    node = node->right_;
  }
  out_file.close();
}

Node *SkipList::GetFirstNode() const {
  Node *p = head_, *q = nullptr;
  while (p) {
    q = p;
    p = p->down_;
  }
  return q;
}
