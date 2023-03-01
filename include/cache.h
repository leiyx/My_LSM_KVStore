// 通用缓存的实现
#ifndef CACHE_H
#define CACHE_H

#include <algorithm>
#include <cstddef>
#include <functional>
#include <limits>
#include <memory>
#include <mutex>
#include <unordered_map>

#include "cache_policy.h"

namespace caches {
/**
 * @brief 固定长度的通用缓存器，该缓存器能使用不同缓存策略：lru、lfu、fifo
 * @tparam Key 键类型
 * @tparam Value 值类型
 * @tparam Policy 缓存策略
 */
template <typename Key, typename Value,
          template <typename> class Policy = NoCachePolicy>
class FixedSizedCache {
 public:
  using iterator = typename std::unordered_map<Key, Value>::iterator;
  using const_iterator =
      typename std::unordered_map<Key, Value>::const_iterator;
  using operation_guard = typename std::lock_guard<std::mutex>;
  using Callback =
      typename std::function<void(const Key &key, const Value &value)>;

  /**
   * @brief Construct a new Fixed Sized Cache object
   * @param[in] max_size cache的最大容量
   * @param[in] policy 使用的缓存策略
   * @param[in] OnErase 删除元素的回调函数
   */
  explicit FixedSizedCache(
      size_t max_size, const Policy<Key> policy = Policy<Key>{},
      Callback OnErase = [](const Key &, const Value &) {})
      : cache_policy_{policy},
        cache_cap_{max_size},
        on_erase_callback_{OnErase} {
    if (cache_cap_ == 0) {
      throw std::invalid_argument{"Size of the cache should be non-zero"};
    }
  }

  ~FixedSizedCache() noexcept { Clear(); }

  /**
   * @brief 将一个elem(<key, value>)放入缓存
   */
  void Put(const Key &key, const Value &value) noexcept {
    operation_guard lock{mutex_};
    auto elem_it = FindElem(key);

    if (elem_it == cache_items_map_.end()) {
      if (cache_items_map_.size() + 1 > cache_cap_) {
        auto disp_candidate_key = cache_policy_.ReplCandidate();

        Erase(disp_candidate_key);
      }

      Insert(key, value);
    } else {
      Update(key, value);
    }
  }

  std::pair<const_iterator, bool> TryGet(const Key &key) const noexcept {
    operation_guard lock{mutex_};
    return GetInternal(key);
  }

  /**
   * @brief 从缓存中读取一个元素如果存在的话
   * @details
   * @param[in] key
   * @return const Value&
   */
  const Value &Get(const Key &key) const {
    operation_guard lock{mutex_};
    auto elem = GetInternal(key);

    if (elem.second) {
      return elem.first->second;
    } else {
      throw std::range_error{"No such element in the cache"};
    }
  }

  bool Cached(const Key &key) const noexcept {
    operation_guard lock{mutex_};
    return FindElem(key) != cache_items_map_.cend();
  }

  std::size_t Size() const {
    operation_guard lock{mutex_};
    return cache_items_map_.size();
  }

  /**
   * @brief 移除指定key的elem
   * @details
   * @param[in] key
   * @return true 移除成功
   * @return false 没有该key
   */
  bool Remove(const Key &key) {
    operation_guard lock{mutex_};

    auto elem = FindElem(key);

    if (elem == cache_items_map_.end()) {
      return false;
    }

    Erase(elem);

    return true;
  }

 protected:
  /**
   * @brief 重置缓存器
   */
  void Clear() {
    operation_guard lock{mutex_};

    std::for_each(begin(), end(), [&](const std::pair<const Key, Value> &el) {
      cache_policy_.Erase(el.first);
    });
    cache_items_map_.clear();
  }

  const_iterator begin() const noexcept { return cache_items_map_.cbegin(); }
  const_iterator end() const noexcept { return cache_items_map_.cend(); }

 protected:
  void Insert(const Key &key, const Value &value) {
    cache_policy_.Insert(key);
    cache_items_map_.emplace(std::make_pair(key, value));
  }

  void Erase(const_iterator elem) {
    cache_policy_.Erase(elem->first);
    on_erase_callback_(elem->first, elem->second);
    cache_items_map_.erase(elem);
  }

  void Erase(const Key &key) {
    auto elem_it = FindElem(key);

    Erase(elem_it);
  }

  void Update(const Key &key, const Value &value) {
    cache_policy_.Touch(key);
    cache_items_map_[key] = value;
  }

  const_iterator FindElem(const Key &key) const {
    return cache_items_map_.find(key);
  }

  std::pair<const_iterator, bool> GetInternal(const Key &key) const noexcept {
    auto elem_it = FindElem(key);

    if (elem_it != end()) {
      cache_policy_.Touch(key);
      return {elem_it, true};
    }

    return {elem_it, false};
  }

 private:
  // 存储element:<key, value>的数据结构
  std::unordered_map<Key, Value> cache_items_map_;
  mutable Policy<Key> cache_policy_;  // 缓存策略
  mutable std::mutex mutex_;          // 互斥锁
  size_t cache_cap_;                  // 缓存容量
  Callback on_erase_callback_;        // 淘汰元素时调用的回调
};
}  // namespace caches

#endif  // CACHE_H
