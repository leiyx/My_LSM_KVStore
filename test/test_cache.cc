#include <assert.h>

#include "cache.h"
#include "fifo_cache_policy.h"
#include "lfu_cache_policy.h"
#include "lru_cache_policy.h"
template <typename KEY, typename VALUE>
using fifo_cache_t =
    typename caches::FixedSizedCache<KEY, VALUE, caches::FIFOCachePolicy>;
template <typename KEY, typename VALUE>
using lru_cache_t =
    typename caches::FixedSizedCache<KEY, VALUE, caches::LRUCachePolicy>;
template <typename KEY, typename VALUE>
using lfu_cache_t =
    typename caches::FixedSizedCache<KEY, VALUE, caches::LFUCachePolicy>;

void TestFIFO() {
  fifo_cache_t<int, int> fc(2);
  fc.Put(1, 10);
  fc.Put(2, 20);
  assert(fc.Cached(1) == true);
  assert(fc.Cached(2) == true);
  fc.Get(1);
  fc.Put(3, 30);
  assert(fc.Cached(1) == false);
  assert(fc.Cached(2) == true);
  assert(fc.Cached(3) == true);
}
void TestLRU() {
  lru_cache_t<int, int> lruc(2);
  lruc.Put(1, 10);
  lruc.Put(2, 20);
  assert(lruc.Cached(1) == true);
  assert(lruc.Cached(2) == true);
  lruc.Get(1);
  lruc.Put(3, 30);
  assert(lruc.Cached(1) == true);
  assert(lruc.Cached(2) == false);
  assert(lruc.Cached(3) == true);
}
void TestLFU() {
  lfu_cache_t<int, int> lfuc(2);
  lfuc.Put(1, 10);
  lfuc.Put(2, 20);
  assert(lfuc.Cached(1) == true);
  assert(lfuc.Cached(2) == true);
  lfuc.Get(1);
  lfuc.Get(1);
  lfuc.Get(2);
  lfuc.Put(3, 30);
  assert(lfuc.Cached(1) == true);
  assert(lfuc.Cached(2) == false);
  assert(lfuc.Cached(3) == true);
}
int main() {
  TestFIFO();
  TestLFU();
  TestLRU();

  return 0;
}