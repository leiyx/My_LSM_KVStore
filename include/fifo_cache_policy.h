/**
 * \file
 * \brief FIFO cache policy implementation
 */
#ifndef FIFO_CACHE_POLICY_H
#define FIFO_CACHE_POLICY_H

#include <list>
#include <unordered_map>

#include "cache_policy.h"

namespace caches {

/**
 * @brief FIFO缓存策略实现
 * @details 先进来的键先淘汰
 * 初始状态：A -> B -> C -> ...
 * 插入新key： X
 * 新状态（替换A）：B -> C -> ... -> X -> ...
 * @tparam Key 键类型
 */
template <typename Key>
class FIFOCachePolicy : public ICachePolicy<Key> {
 public:
  using fifo_iterator = typename std::list<Key>::const_iterator;

  FIFOCachePolicy() = default;
  ~FIFOCachePolicy() = default;

  // 向cache中插入元素
  void Insert(const Key &key) override {
    fifo_queue_.emplace_front(key);
    key_lookup_[key] = fifo_queue_.begin();
  }

  // 请求cache中的一个元素
  void Touch(const Key &key) noexcept override {
    // nothing to do here in the FIFO strategy
    // 使用key后的影响，如果是FIFO策略，则什么都不做。
  }
  // 从cache中删除元素
  void Erase(const Key &key) noexcept override {
    auto element = key_lookup_[key];
    fifo_queue_.erase(element);
    key_lookup_.erase(key);
  }

  // 返回下一个被替换的key
  const Key &ReplCandidate() const noexcept override {
    return fifo_queue_.back();
  }

 private:
  std::list<Key> fifo_queue_;
  std::unordered_map<Key, fifo_iterator> key_lookup_;
};
}  // namespace caches

#endif  // FIFO_CACHE_POLICY_H
