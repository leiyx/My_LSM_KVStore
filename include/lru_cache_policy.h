
#ifndef LRU_CACHE_POLICY_H
#define LRU_CACHE_POLICY_H

#include <list>
#include <unordered_map>

#include "cache_policy.h"

namespace caches {

/**
 * @brief LRU (Least Recently Used）缓存策略实现
 * @details 淘汰最久未使用的元素
 * A, B, C
 * 使用A 、使用B，此时备选淘汰的是C
 * 使用B、使用C，此时备选淘汰的是A
 * 放入新元素D，此时淘汰A
 * B，C，D
 * @tparam Key 键的类型
 */
template <typename Key>
class LRUCachePolicy : public ICachePolicy<Key> {
 public:
  using lru_iterator = typename std::list<Key>::iterator;

  LRUCachePolicy() = default;
  ~LRUCachePolicy() = default;

  void Insert(const Key &key) override {
    lru_queue_.emplace_front(key);
    key_finder_[key] = lru_queue_.begin();
  }

  void Touch(const Key &key) override {
    // 将最近访问过的元素移动到lru_queue_的开头，表示该元素现在的淘汰优先级最低
    lru_queue_.splice(lru_queue_.begin(), lru_queue_, key_finder_[key]);
  }

  void Erase(const Key &) noexcept override {
    // 移除一个最久未使用的元素
    key_finder_.erase(lru_queue_.back());
    lru_queue_.pop_back();
  }

  // 返回一个备选淘汰元素
  const Key &ReplCandidate() const noexcept override {
    return lru_queue_.back();
  }

 private:
  std::list<Key> lru_queue_;
  std::unordered_map<Key, lru_iterator> key_finder_;
};
}  // namespace caches
#endif  // LRU_CACHE_POLICY_H
