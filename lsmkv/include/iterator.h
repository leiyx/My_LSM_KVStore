#ifndef STORAGE_XDB_INCLUDE_ITERATOR_H_
#define STORAGE_XDB_INCLUDE_ITERATOR_H_

#include <string_view>
#include "include/status.h"

namespace lsmkv {

class ReadOption;

class Iterator {
 public:
    Iterator();

    Iterator(const Iterator&) = delete;
    Iterator& operator=(const Iterator&) = delete;

    virtual ~Iterator();

    virtual bool Valid() const = 0;

    virtual std::string_view Key() const = 0;

    virtual std::string_view Value() const = 0;

    virtual void Next() = 0;

    virtual void Prev() = 0;

    virtual void Seek(std::string_view key) = 0;

    virtual void SeekToFirst() = 0;

    virtual void SeekToLast() = 0;

    virtual Status status() = 0;

    using CleanupFunction = void(*)(void* arg1, void* arg2);
    void AppendCleanup (CleanupFunction fun, void* arg1, void* arg2);
 private:
    struct CleanupNode {
        void Run() {
            (*fun)(arg1, arg2);
        }
        CleanupFunction fun;
        void* arg1;
        void* arg2;
        CleanupNode* next;
    };
    CleanupNode cleanup_head_;
};

Iterator* NewEmptyIterator();

Iterator* NewErrorIterator(Status status);

Iterator* NewTwoLevelIterator(Iterator* index_iter, 
    Iterator* (*block_funtion)(void* arg, const ReadOption& option, std::string_view handle_contents),
    void* arg, const ReadOption& option);
}
#endif // STORAGE_XDB_INCLUDE_ITERATOR_H_