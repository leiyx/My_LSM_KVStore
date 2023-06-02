#ifndef STORAGE_XDB_INCLUDE_SSTABLE_BUILDER_H_
#define STORAGE_XDB_INCLUDE_SSTABLE_BUILDER_H_

#include "include/option.h"
#include "util/file.h"
#include "db/sstable/table_cache.h"

namespace lsmkv {

class BlockBuilder;
class BlockHandle;
class Iterator;
class FileMeta;

class SSTableBuilder {
 public:
    SSTableBuilder(const Option& option,WritableFile* file);

    SSTableBuilder(const SSTableBuilder&) = delete;
    SSTableBuilder& operator=(const SSTableBuilder&) = delete;

    ~SSTableBuilder();

    void Add(std::string_view key, std::string_view value);

    void Flush();
    
    Status Finish();

    Status status() const;

    uint64_t FileSize() const;

    uint64_t NumEntries() const;
 private:
    bool ok() const { return status().ok(); }
    void WriteBlock(BlockBuilder* builder, BlockHandle* handle);

    void WriteRawBlock(std::string_view contents,
             CompressType type, BlockHandle* handle);
    struct Rep;
    Rep* rep_;
};

Status BuildSSTable(const std::string name, const Option& option, 
      TableCache* table_cache, Iterator* iter, FileMeta* meta);
}

#endif // STORAGE_XDB_INCLUDE_SSTABLE_BUILDER_H_