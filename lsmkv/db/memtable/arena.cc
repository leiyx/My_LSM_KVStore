#include "db/memtable/arena.h"

#include <cassert>

namespace lsmkv {

static constexpr int kBlockSize = 4096;

char* Arena::AllocateNewBlock(size_t bytes) {
  char* block = new char[bytes];
  blocks_.push_back(block);
  memory_used_.fetch_add(bytes + sizeof(char*), std::memory_order_release);
  return block;
}

char* Arena::Allocate(size_t bytes) {
  if (alloc_bytes_remaining_ < bytes) {
    return AllocateFallBack(bytes);
  }
  char* result = alloc_ptr_;
  alloc_ptr_ += bytes;
  alloc_bytes_remaining_ -= bytes;
  return result;
}

char* Arena::AllocateAlign(size_t bytes) {
  const int align = (sizeof(void*) > 8) ? sizeof(void*) : 8;
  static_assert((align & (align - 1)) == 0,
                "Pointer size should be a power of 2");
  size_t buf_low_bit = reinterpret_cast<size_t>(alloc_ptr_) & (align - 1);
  size_t need = bytes + (align - buf_low_bit);
  char* result;
  if (need <= alloc_bytes_remaining_) {
    result = alloc_ptr_ + (align - buf_low_bit);
    alloc_ptr_ += need;
    alloc_bytes_remaining_ -= need;
  } else {
    result = AllocateFallBack(bytes);
  }
  assert((reinterpret_cast<size_t>(result) & (align - 1)) == 0);
  return result;
}

char* Arena::AllocateFallBack(size_t bytes) {
  if (bytes > kBlockSize / 4) {
    return AllocateNewBlock(bytes);
  }
  alloc_ptr_ = AllocateNewBlock(kBlockSize);
  alloc_bytes_remaining_ = kBlockSize;
  char* result = alloc_ptr_;
  alloc_ptr_ += bytes;
  alloc_bytes_remaining_ -= bytes;
  return result;
}

}  // namespace lsmkv