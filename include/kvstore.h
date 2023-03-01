#include <map>
#include <mutex>
#include <queue>
#include <set>
#include <shared_mutex>

#include "cache.h"
#include "kvstore_api.h"
#include "options.h"
#include "skiplist.h"
#include "table_cache.h"
#include "thread_pool.h"

#ifdef FIFO
#include "fifo_cache_policy.h"
template <typename KEY, typename VALUE>
using cache_t =
    typename caches::FixedSizedCache<KEY, VALUE, caches::FIFOCachePolicy>;
#elif defined LRU
#include "lru_cache_policy.h"
template <typename KEY, typename VALUE>
using cache_t =
    typename caches::FixedSizedCache<KEY, VALUE, caches::LRUCachePolicy>;
#elif defined LFU
#include "lfu_cache_policy.h"
template <typename KEY, typename VALUE>
using cache_t =
    typename caches::FixedSizedCache<KEY, VALUE, caches::LFUCachePolicy>;
#endif

class KVStore : public KVStoreAPI {
 public:
  KVStore(const std::string &dir);
  ~KVStore();

  void Put(uint64_t key, const std::string &val,
           bool to_cache = false) override;
  // 将Put函数封装为任务，以便丢进线程池
  void PutTask(uint64_t key, const std::string &val, bool to_cache = false);

  std::string Get(uint64_t key) override;
  // 将Get函数封装为任务，以便丢进线程池。返回一个包含key对应val的future对象
  std::future<std::string> GetTask(uint64_t key);

  /**
   * @brief 删除键值对
   * @details
   * 由于LSM树的特性，删除操作仅是给该key做一个删除标记，所以本质上还是调用Put接口
   * @param[in] key 要删除键值对的key
   * @return true key存在，删除成功
   * @return false key不存在，删除失败
   */
  bool Del(uint64_t key) override;
  // 将Del函数封装为任务，以便丢进线程池
  void DelTask(uint64_t key);

  void Reset() override;

 private:
  /**
   * @brief immutable memtable ->  Level0 SST文件
   * @details 会调用MajorCompaction(1)检查Level0是否需要进行MajorCompaction
   */
  void MinorCompaction();

  /**
   * @brief
   * 如果level-1层SST文件数量超过限制，则将level-1层的SST文件与level层的SST文件合并放到level层
   * @details 采用多路归并排序
   * @param[in] level 检查是否要进行compaction的层数
   */
  void MajorCompaction(int level);

  /**
   * @brief
   * 合并SST文件，需要将其读到内存中在进行合并后，再写回。这里负责写回的操作。
   * @details 只会被MajorCompaction函数调用
   * @param[in] level
   * @param[in] time_stamp_
   * @param[in] numPair
   * @param[in] newTable
   */
  void WriteToFile(int level, uint64_t time_stamp_, uint64_t numPair,
                   std::map<int64_t, std::string> &newTable);

 private:
  enum mode { normal, compact, exits };

  std::shared_ptr<SkipList> mem_table_;
  std::shared_ptr<SkipList> immutable_table_;

  std::string dir_;                 // SSTable文件存储目录
  std::vector<int> level_num_vec_;  // 记录对应层的文件数目
  std::vector<std::set<TableCache>>
      sstable_meta_info_;  // 记录所有SSTable文件的元信息
  mode kv_store_mode_;     // 存储引擎工作模式
  ThreadPool pool_{4};     // 线程池
  cache_t<uint64_t, std::string> cache_;  // 缓存器
  // 互斥与同步相关
  std::condition_variable cv_;
  std::mutex m_;
  std::shared_mutex read_write_m_;
};
