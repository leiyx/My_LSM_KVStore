// 缓存策略接口声明
#ifndef CACHE_POLICY_H
#define CACHE_POLICY_H

#include <unordered_set>

namespace caches {

// 缓存策略抽象基类
// Key代表使用哪种缓存策略
template <typename Key>
class ICachePolicy {
 public:
  virtual ~ICachePolicy() = default;

  // 插入该元素
  virtual void Insert(const Key &key) = 0;

  // 访问该元素
  virtual void Touch(const Key &key) = 0;

  // 删除该元素
  virtual void Erase(const Key &key) = 0;

  // 返回一个根据选择的策略，应当被淘汰的元素
  virtual const Key &ReplCandidate() const = 0;
};

// 无缓存策略，淘汰底层容器的第一个元素，考虑无序map等容器，所以这不是fifo策略
template <typename Key>
class NoCachePolicy : public ICachePolicy<Key> {
 public:
  NoCachePolicy() = default;
  ~NoCachePolicy() noexcept override = default;

  void Insert(const Key &key) override { key_storage_.emplace(key); }

  void Touch(const Key &key) noexcept override {
    // do not do anything
  }

  void Erase(const Key &key) noexcept override { key_storage_.erase(key); }

  // 返回候选置换元素的key，因为没有具体的缓存策略，所以这里随便返回一个就行
  const Key &ReplCandidate() const noexcept override {
    return *key_storage_.cbegin();
  }

 private:
  std::unordered_set<Key> key_storage_;
};
}  // namespace caches

#endif  // CACHE_POLICY_H
