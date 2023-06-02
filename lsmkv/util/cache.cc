#include "include/cache.h"

#include <cassert>
#include <cstdint>
#include <cstring>

#include "util/MurmurHash3.h"
#include "util/mutex.h"
#include "util/thread_annotations.h"
namespace lsmkv {

struct LRUHandle {
  void* value;
  void (*deleter)(std::string_view, void* value);
  LRUHandle* next_hash;
  LRUHandle* next;
  LRUHandle* prev;
  size_t charge;
  size_t key_length;
  uint32_t refs;
  uint32_t hash;
  bool in_cache;
  char key_data[1];

  std::string_view key() const {
    assert(next != this);
    return std::string_view(key_data, key_length);
  }
};

class HandleTable {
 public:
  HandleTable() : element_num_(0), capacity_(0), list_(nullptr) { Resize(); }
  ~HandleTable() { delete[] list_; }

  LRUHandle* Lookup(std::string_view key, uint32_t hash) {
    return *Find(key, hash);
  }

  LRUHandle* Insert(LRUHandle* h) {
    LRUHandle** ptr = Find(h->key(), h->hash);
    LRUHandle* old = *ptr;
    h->next_hash = (old == nullptr ? nullptr : old->next_hash);
    *ptr = h;
    if (old == nullptr) {
      ++element_num_;
      if (element_num_ > capacity_) {
        Resize();
      }
    }
    return old;
  }

  LRUHandle* Remove(std::string_view key, uint32_t hash) {
    LRUHandle** ptr = Find(key, hash);
    LRUHandle* result = *ptr;
    if (result != nullptr) {
      *ptr = result->next_hash;
      --element_num_;
    }
    return result;
  }

  void Resize() {
    uint32_t new_capacity = 4;
    while (new_capacity < element_num_) {
      new_capacity *= 2;
    }
    LRUHandle** new_list = new LRUHandle*[new_capacity];
    std::memset(new_list, 0, sizeof(new_list[0]) * new_capacity);

    uint32_t count = 0;
    for (uint32_t i = 0; i < capacity_; i++) {
      LRUHandle* h = list_[i];
      while (h != nullptr) {
        LRUHandle* next = h->next_hash;
        uint32_t hash = h->hash;
        LRUHandle** ptr = &new_list[hash & (new_capacity - 1)];
        h->next_hash = *ptr;
        *ptr = h;
        h = next;
        count++;
      }
    }
    assert(element_num_ == count);
    delete[] list_;
    list_ = new_list;
    capacity_ = new_capacity;
  }

 private:
  LRUHandle** Find(std::string_view key, uint32_t hash) {
    LRUHandle** ptr = &list_[hash & (capacity_ - 1)];
    while (*ptr != nullptr && ((*ptr)->hash != hash || (*ptr)->key() != key)) {
      ptr = &(*ptr)->next_hash;
    }
    return ptr;
  }
  LRUHandle** list_;
  uint32_t element_num_;
  uint32_t capacity_;
};

class LRUCache {
 public:
  LRUCache() : capacity_(0), usage_(0) {
    lru_.prev = &lru_;
    in_use_.prev = &in_use_;
    lru_.next = &lru_;
    in_use_.next = &in_use_;
  }
  ~LRUCache();

  void SetCapacity(size_t capacity) { capacity_ = capacity; }

  Cache::Handle* Lookup(std::string_view key, uint32_t hash);
  Cache::Handle* Insert(std::string_view key, uint32_t hash, void* value,
                        size_t charge,
                        void (*deleter)(std::string_view key, void* value));
  void Release(Cache::Handle* handle);
  void Erase(std::string_view key, uint32_t hash);

 private:
  void Ref(LRUHandle* e);
  void Unref(LRUHandle* e);
  void LRU_Append(LRUHandle* list, LRUHandle* e);
  void LRU_Remove(LRUHandle* e);
  void FinishErase(LRUHandle* e);

  Mutex mu_;
  size_t capacity_;
  size_t usage_ GUARDED_BY(mu_);
  LRUHandle lru_ GUARDED_BY(mu_);
  LRUHandle in_use_ GUARDED_BY(mu_);
  HandleTable table_ GUARDED_BY(mu_);
};

LRUCache::~LRUCache() {
  LRUHandle* e = lru_.next;
  while (e != &lru_) {
    LRUHandle* next = e->next;
    assert(e->in_cache);
    e->in_cache = false;
    assert(e->refs == 1);
    Unref(e);
    e = next;
  }
}
Cache::Handle* LRUCache::Lookup(std::string_view key, uint32_t hash) {
  MutexLock lock(&mu_);
  LRUHandle* e = table_.Lookup(key, hash);
  if (e != nullptr) {
    Ref(e);
  }
  return reinterpret_cast<Cache::Handle*>(e);
}

Cache::Handle* LRUCache::Insert(std::string_view key, uint32_t hash,
                                void* value, size_t charge,
                                void (*deleter)(std::string_view key,
                                                void* value)) {
  MutexLock lock(&mu_);

  LRUHandle* handle =
      reinterpret_cast<LRUHandle*>(malloc(sizeof(LRUHandle) - 1 + key.size()));
  handle->value = value;
  handle->deleter = deleter;
  handle->hash = hash;
  handle->charge = charge;
  handle->key_length = key.size();
  handle->refs = 1;
  handle->in_cache = false;
  memcpy(handle->key_data, key.data(), key.size());

  if (capacity_ > 0) {
    ++handle->refs;
    handle->in_cache = true;
    LRU_Append(&in_use_, handle);
    usage_ += charge;
    FinishErase(table_.Insert(handle));
  } else {
    handle->next = nullptr;
  }

  while (usage_ > capacity_ && lru_.next != &lru_) {
    LRUHandle* old = lru_.next;
    assert(old->refs == 1);
    FinishErase(table_.Remove(old->key(), old->hash));
  }

  return reinterpret_cast<Cache::Handle*>(handle);
}

void LRUCache::Release(Cache::Handle* handle) {
  MutexLock lock(&mu_);
  Unref(reinterpret_cast<LRUHandle*>(handle));
}

void LRUCache::Erase(std::string_view key, uint32_t hash) {
  MutexLock lock(&mu_);
  FinishErase(table_.Remove(key, hash));
}

void LRUCache::LRU_Append(LRUHandle* list, LRUHandle* e) {
  e->next = list;
  e->prev = list->prev;
  e->next->prev = e;
  e->prev->next = e;
}

void LRUCache::LRU_Remove(LRUHandle* e) {
  e->next->prev = e->prev;
  e->prev->next = e->next;
}

void LRUCache::Ref(LRUHandle* e) {
  if (e->refs == 1 && e->in_cache) {
    LRU_Remove(e);
    LRU_Append(&in_use_, e);
  }
  ++e->refs;
}

void LRUCache::Unref(LRUHandle* e) {
  --e->refs;
  if (e->refs == 0) {
    assert(!e->in_cache);
    (*e->deleter)(e->key(), e->value);
    free(e);
  } else if (e->in_cache && e->refs == 1) {
    LRU_Remove(e);
    LRU_Append(&lru_, e);
  }
}

void LRUCache::FinishErase(LRUHandle* e) EXCLUSIVE_LOCKS_REQUIRED(mu_) {
  if (e != nullptr) {
    assert(e->in_cache);
    LRU_Remove(e);
    e->in_cache = false;
    usage_ -= e->charge;
    Unref(e);
  }
}

static constexpr int KNumShardBits = 1;
static constexpr int KNumShard = 1 << KNumShardBits;

class ShardLRUCache : public Cache {
 public:
  ShardLRUCache(size_t capacity) {
    size_t shard_capacity = (capacity + (KNumShard - 1)) / KNumShard;
    for (int i = 0; i < KNumShard; i++) {
      shard_[i].SetCapacity(shard_capacity);
    }
  }
  Handle* Lookup(std::string_view key) override {
    uint32_t hash = Hash(key);
    return shard_[Shard(hash)].Lookup(key, hash);
  }

  void Release(Handle* handle) override {
    LRUHandle* e = reinterpret_cast<LRUHandle*>(handle);
    return shard_[Shard(e->hash)].Release(handle);
  }

  void Erase(std::string_view key) override {
    uint32_t hash = Hash(key);
    shard_[Shard(hash)].Erase(key, hash);
  }

  void* Value(Handle* handle) override {
    return reinterpret_cast<LRUHandle*>(handle)->value;
  }

  Handle* Insert(std::string_view key, void* value, size_t charge,
                 void (*deleter)(std::string_view key, void* value)) override {
    uint32_t hash = Hash(key);
    return shard_[Shard(hash)].Insert(key, hash, value, charge, deleter);
  }

 private:
  uint32_t Hash(std::string_view key) {
    return murmur3::MurmurHash3_x86_32(key.data(), key.size(), 0);
  }
  uint32_t Shard(uint32_t hash) { return hash >> (32 - KNumShardBits); }
  LRUCache shard_[KNumShard];
};

Cache* NewLRUCache(size_t capacity) { return new ShardLRUCache(capacity); }
}  // namespace lsmkv