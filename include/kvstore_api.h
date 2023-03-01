#ifndef KVSTORE_API_H
#define KVSTORE_API_H

#include <cstdint>
#include <string>

/**
 * @brief 存储引擎向外部提供的API类
 * @details 纯虚类，需要其子类(具体的存储引擎实现类)实现Get、Put、Del、Reset接口
 */
class KVStoreAPI {
 public:
  /**
   * @brief 构造KVStoreAPI对象
   * @details Level0最多只有两个SSTable文件，键值范围可能重叠。
   * 其余层可能有多个SSTable文件，键值范围不重叠。
   * @param[in] dir 所有SSTable文件存放的目录。
   * 其下有n+1个目录，分别对应0-n层：
   * --dir
      --Level0
        --SSTable0
        --SSTable1
      --Level1
        --SSTable0
        --SSTable1
        --SSTable2
        --SStable3
      ...
      --Leveln
        --SSTable0
        ...
        --SSTable(2^{n+1}-1)
   */
  KVStoreAPI(const std::string &dir) {}
  KVStoreAPI() = delete;

  /**
   * @brief 插入键值对或更新键值对
   * @param[in] key 键
   * @param[in] val 值
   * @param[in] to_cache 是否缓存该元素<key,val>
   */
  virtual void Put(uint64_t key, const std::string &val,
                   bool to_cache = false) = 0;

  /**
   * @brief 查找键值对
   * @param[in] key 要查找的key
   * @return std::string 返回key对应的val，如果key不存在，返回""
   */
  virtual std::string Get(uint64_t key) = 0;

  /**
   * @brief 删除键值对
   * @param[in] key 要删除键值对的key
   * @return true key存在，删除成功
   * @return false key不存在，删除失败
   */
  virtual bool Del(uint64_t key) = 0;

  /**
   * @brief 重置kvstore
   * @details 移除所有键值对元素，包括Memtable、Immutable
   * Memtable和所有SSTable文件
   */
  virtual void Reset() = 0;
};
#endif