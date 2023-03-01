
#ifndef LFU_CACHE_POLICY_H
#define LFU_CACHE_POLICY_H

#include <cstddef>
#include <iostream>
#include <map>
#include <unordered_map>

#include "cache_policy.h"

namespace caches {
/**
 * @brief LFU(Least frequently used) 缓存策略实现
 * @details 淘汰使用次数最少的键
 * @tparam Key 键的类型
 */
template <typename Key>
class LFUCachePolicy : public ICachePolicy<Key> {
 public:
  using lfu_iterator = typename std::multimap<std::size_t, Key>::iterator;

  LFUCachePolicy() = default;
  ~LFUCachePolicy() override = default;

  void Insert(const Key &key) override {
    constexpr std::size_t INIT_VAL = 1;
    // 给每个新key设置初始次数为1
    lfu_storage_[key] = frequency_storage_.emplace_hint(
        frequency_storage_.cbegin(), INIT_VAL, key);
  }

  void Touch(const Key &key) override {
    // 拿到一个key以前的使用次数
    auto elem_for_update = lfu_storage_[key];
    auto updated_elem =
        std::make_pair(elem_for_update->first + 1, elem_for_update->second);
    frequency_storage_.erase(elem_for_update);
    lfu_storage_[key] = frequency_storage_.emplace_hint(
        frequency_storage_.cend(), std::move(updated_elem));
  }

  void Erase(const Key &key) noexcept override {
    frequency_storage_.erase(lfu_storage_[key]);
    lfu_storage_.erase(key);
  }

  const Key &ReplCandidate() const noexcept override {
    // frequency_storage_开头的key就是使用次数最少的key
    return frequency_storage_.cbegin()->second;
  }

 private:
  std::multimap<std::size_t, Key> frequency_storage_;
  std::unordered_map<Key, lfu_iterator> lfu_storage_;
};
}  // namespace caches

#endif  // LFU_CACHE_POLICY_H
