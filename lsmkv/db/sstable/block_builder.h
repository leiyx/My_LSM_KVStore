#ifndef STORAGE_XDB_DB_SSTABLE_BLOCK_BUILDER_H_
#define STORAGE_XDB_DB_SSTABLE_BLOCK_BUILDER_H_

#include <vector>

#include <string_view>
#include "include/option.h"
namespace lsmkv {

class BlockBuilder {
 public:
    explicit BlockBuilder(const Option* option);

    BlockBuilder(const BlockBuilder &) = delete;
    BlockBuilder &operator=(const BlockBuilder &) = delete;

    void Add(std::string_view key, std::string_view value);

    std::string_view Finish();

    size_t ByteSize() const;

    void Reset();

    bool Empty() const { return buffer_.empty(); };
private:
    const Option* option_;
    std::string buffer_;
    // restart is a record that key has full length.
    // use for speed up searching.
    std::vector<uint32_t> restarts_;
    // num of key after last restrat
    int counter_;
    // last key has been addd.
    std::string last_key_;
    bool finished_;
};
}

#endif