#ifndef ARENA_H
#define ARENA_H
/**
 * @file arena.h
 * @brief 内存分配器，管理跳表的节点的内存
 */
#include <atomic>
#include <vector>

namespace lsmkv {

class Arena {
 public:
  Arena() : alloc_ptr_(nullptr), alloc_bytes_remaining_(0), memory_used_(0) {}
  ~Arena() {
    for (char* block : blocks_) delete[] block;
  }

  /**
   * @brief 分配bytes个字节的连续内存块
   * @note 内存未对齐
   * @param[in] bytes 申请字节数
   * @return char* 指向分配内存块的指针
   */
  char* Allocate(size_t bytes);

  /**
   * @brief 分配bytes个字节的连续内存块
   * @note 内存已对齐
   * @param[in] bytes 申请字节数
   * @return char* 指向分配内存块的指针
   */
  char* AllocateAlign(size_t bytes);

  /**
   * @brief 记录本内存分配器已使用的内存大小
   */
  size_t MemoryUsed() const {
    return memory_used_.load(std::memory_order_relaxed);
  }

 private:
  /**
   * @brief 当前块中剩余可用内存不能满足需要时，调用该函数
   */
  char* AllocateFallBack(size_t bytes);

  /**
   * @brief 由内存分配器向操作系统申请一个新内存块，由AllocateFallBack调用
   */
  char* AllocateNewBlock(size_t bytes);

 private:
  /// 记录内存分配器当前使用的内存块
  char* alloc_ptr_;
  /// 记录当前内存块中剩余可用字节数
  size_t alloc_bytes_remaining_;
  /// 记录所有指向内存块的指针
  std::vector<char*> blocks_;
  /// 记录内存分配器使用的内存大小
  std::atomic<size_t> memory_used_;
};

}  // namespace lsmkv

#endif  // ARENA_H