#ifndef TABLE_CACHE_H
#define TABLE_CACHE_H
#include <assert.h>

#include <map>

#include "Murmur_hash3.h"
#include "skiplist.h"

class TableCache {
 public:
  TableCache() { sst_path_ = ""; }
  TableCache(const std::string& file_name);

  /**
   * @brief TableCache类的小于运算符重载
   * @details 排序优先级：
   * 1. 时间戳小的TableCache对象排前面
   * 2. 如果时间戳相等，则GetMinKey更小的TableCache对象排前面
   */
  bool operator<(const TableCache& temp) const {
    return GetTimestamp() == temp.GetTimestamp()
               ? GetMinKey() < temp.GetMinKey()
               : GetTimestamp() < temp.GetTimestamp();
  }

  /**
   * @brief TableCache类的大于运算符重载, 排序优先级与operator<排序优先级相反
   */
  bool operator>(const TableCache& temp) const {
    return GetTimestamp() == temp.GetTimestamp()
               ? GetMinKey() > temp.GetMinKey()
               : GetTimestamp() > temp.GetTimestamp();
  }

  /**
   * @brief 据输入的key，寻找该SST文件中有无对应的val
   * @param[in] key 查找的key
   * @return string 如果key存在则返回对应val，如果key不存在则返回""
   */
  std::string GetValue(uint64_t key) const;

  /**
   * @brief 打开此SST文件，更新缓存在内存中的该文件对应的各项元信息
   */
  void Open();

  /**
   * @brief 遍历文件，将该SST文件的键值对全部读进内存
   * @param[out] pair 键值对读进内存的存放位置
   */
  void Traverse(std::map<int64_t, std::string>& pair) const;

  // 一组成员getter接口
  std::string GetFileName() const { return sst_path_; }
  uint64_t GetTimestamp() const { return time_and_size_[0]; }
  uint64_t GetNumPair() const { return time_and_size_[1]; }
  int64_t GetMaxKey() const { return min_max_key_[1]; }
  int64_t GetMinKey() const { return min_max_key_[0]; }

 private:
  std::string sst_path_;                        // SST文件路径及文件名
  uint64_t time_and_size_[2];                   // 时间戳和键值对数量
  int64_t min_max_key_[2];                      // 最小最大键
  std::bitset<81920> bloom_filter_;             // 布隆过滤器
  std::map<int64_t, uint32_t> key_offset_map_;  // 储存每个key对应的偏移量
};

#endif  // TABLE_CACHE_H
