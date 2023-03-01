// KV存储引擎的参数设置
#ifndef OPTIONS_H
#define OPTIONS_H
#include <math.h>

#include <string>

namespace options {

// for KVStore
// 删除标记
const std::string kDelSign = "~DELETED~";
/**
 * @brief 计算每层的SST文件个数上限
 * @param[in] i 层数
 * @return int 2^(i+1)
 */
static inline int SSTMaxNumForLevel(int i) { return pow(2, i + 1); }

// for SkipList
// MemTable的内存上限
const int kMemTable = (int)pow(2, 21);
// 跳表的初始大小memory初值
const int kInitialSize = 10272;

// for cache
// "LRU"、"LFU"、"FIFO"三选一
#define LRU
// #define LFU
// #define FIFO
// 缓存器的容量，最多能缓存多少个元素elem（<key, value>）
const int kCacheCap = 100;

}  // namespace options
#endif