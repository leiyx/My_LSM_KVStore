#ifndef MEMTABLE_H
#define MEMTABLE_H

#include "db/format/internal_key.h"
#include "db/memtable/arena.h"
#include "db/memtable/skiplist.h"
#include "include/iterator.h"
#include "include/status.h"

namespace lsmkv {

class MemTable {
 public:
  explicit MemTable(const InternalKeyComparator& cmp);

  MemTable(const MemTable&) = delete;
  MemTable& operator=(const MemTable&) = delete;

  /**
   * @brief 引用计数+1
   */
  void Ref() { refs_++; };

  /**
   * @brief 引用计数-1
   */
  void Unref() {
    refs_--;
    if (refs_ <= 0) {
      delete this;
    }
  }

  /**
   * @brief Put接口实现
   * @details DB的Put和Del接口都会调用此函数
   * @param[in] seq record的序号
   * @param[in] type record的类型
   * @param[in] key record的key
   * @param[in] value record的value
   */
  void Put(SequenceNum seq, RecordType type, std::string_view key,
           std::string_view value);

  /**
   * @brief Get接口实现
   * @param[in] key 查找的key值
   * @param[out] result 查找的结果
   * @param[out] status 查找的状态
        @retval true Get成功
        @retval false Get失败
   */
  bool Get(const LookupKey& key, std::string* result, Status* status);

  /**
   * @brief 生成一个迭代器
   */
  Iterator* NewIterator();

  /**
   * @brief 返回内存表使用的近似内存大小
   */
  size_t ApproximateSize() { return arena_.MemoryUsed(); }

 private:
  struct KeyComparator {
    const InternalKeyComparator comparator;
    explicit KeyComparator(const InternalKeyComparator& cmp)
        : comparator(cmp) {}
    int operator()(const char* a, const char* b) const;
  };

  using Table = SkipList<const char*, KeyComparator>;

  friend class MemTableIterator;

  ~MemTable() { assert(refs_ == 0); };

 private:
  /// 比较器，用于key之间的比较
  KeyComparator comparator_;
  /// 内存分配器，为跳表分配内存
  Arena arena_;
  /// 存放数据的底层结构，跳表
  Table table_;
  /// 引用计数
  int refs_;
};

}  // namespace lsmkv

#endif  // MEMTABLE_H