#ifndef STORAGE_XDB_DB_INCLUDE_CACHE_H_
#define STORAGE_XDB_DB_INCLUDE_CACHE_H_

#include <string_view>

namespace lsmkv {
class Cache {
 public:
    Cache() = default;

    Cache(const Cache&) = delete;
    Cache& operator=(const Cache&) = delete;

    virtual ~Cache() = default;

    // handle is used to handle an entry stored in cache
    struct Handle {};

    // If cache include a record with "key", return it;
    // the handle must be "Release()" after used;
    virtual Handle* Lookup(std::string_view key) = 0;

    // handle must be called after Lookup() or Insert()
    // a handle must not be Release() more than once;
    virtual void Release(Handle* handle) = 0;

    // Erase the record with "key" in cache
    // the record will be kept until the handle point to it is release
    virtual void Erase(std::string_view key) = 0;

    // Return the value stored in handle
    // the handle must not have been Release()
    virtual void* Value(Handle* handle) = 0;

    // insert an record into cache.charge is the size of the record
    // deleter will be call after the record is deleted
    // the handle must be "Release()" after used.
    virtual Handle* Insert(std::string_view key, void* value,
            size_t charge, void (*deleter)(std::string_view key, void* value)) = 0;
};

Cache* NewLRUCache(size_t capacity);

}

#endif // STORAGE_XDB_DB_INCLUDE_CACHE_H_