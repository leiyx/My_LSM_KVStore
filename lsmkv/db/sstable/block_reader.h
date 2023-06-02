#ifndef STORAGE_XDB_DB_SSTABLE_BLOCK_READER_H_
#define STORAGE_XDB_DB_SSTABLE_BLOCK_READER_H_

#include "include/iterator.h"
#include "include/comparator.h"

namespace lsmkv {

struct BlockContents;

class BlockReader {
 public:
    explicit BlockReader(const BlockContents& contents);
    
    ~BlockReader();

    BlockReader(const BlockReader&) = delete;
    BlockReader& operator=(const BlockReader&) = delete;

    Iterator* NewIterator(const Comparator* cmp);
 private:
    class Iter;

    size_t NumRestarts() const;
    const char* data_;
    size_t size_;
    uint32_t restarts_offset_;
    bool owned_;
};

}

#endif // STORAGE_XDB_DB_SSTABLE_BLOCK_READER_H_