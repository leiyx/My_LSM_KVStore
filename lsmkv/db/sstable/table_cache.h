#ifndef STORAGE_XDB_DB_SSTABLE_TABLE_CACHE_H_
#define STORAGE_XDB_DB_SSTABLE_TABLE_CACHE_H_


#include "include/cache.h"
#include "include/env.h"
#include "include/option.h"

namespace lsmkv {

class Iterator;

class TableCache {
 public:
    TableCache(const std::string name, const Option& option, size_t capacity)
            : name_(name), option_(option),
              env_(option.env), cache_(NewLRUCache(capacity)) {}

    ~TableCache() { delete cache_; }
    
    Status Get(const ReadOption& option, uint64_t file_number, uint64_t file_size,
            std::string_view key, void* arg, void (*handle_result)(void*, std::string_view, std::string_view));

    void Evict(uint64_t file_number);

    Iterator* NewIterator(const ReadOption& option, uint64_t file_number, uint64_t file_size);

 private:
    Status FindTable(uint64_t file_number, uint64_t file_size, Cache::Handle** handle);
    
    const std::string name_;
    const Option& option_;
    Env* env_;
    Cache* cache_;
};

}

#endif // STORAGE_XDB_DB_SSTABLE_TABLE_CACHE_H_