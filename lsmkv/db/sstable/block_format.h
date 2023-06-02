#ifndef STORAGE_XDB_DB_SSTABLE_BLOCK_FORMAT_H_
#define STORAGE_XDB_DB_SSTABLE_BLOCK_FORMAT_H_

#include <string_view>
#include "include/status.h"
#include "include/option.h"
namespace lsmkv {

struct BlockContents {
    std::string_view data;
    bool heap_allocated_;
    bool table_cache_;
};
// 1 byte compress type | 4 bytes crc
static const size_t KBlockTailSize = 5;

static const uint64_t KFooterMagicNum = 0xaf41de78ull;

struct BlockHandle {
 public:
    enum {KMaxEncodeLength = 20};

    BlockHandle();
    
    void SetOffset(uint64_t offset) { offset_ = offset; }

    uint64_t GetOffset() const { return offset_; }

    void SetSize(uint64_t size) { size_ = size; }

    uint64_t GetSize() const { return size_; }

    void EncodeTo(std::string* dst);

    Status DecodeFrom(std::string_view* input);
 private:
    uint64_t offset_;
    uint64_t size_;
};

struct Footer {
 public:
    Footer() = default;
    
    enum {KEncodeLength = 2 * BlockHandle::KMaxEncodeLength + 8};
    void SetIndexHandle(const BlockHandle& index_block_handle) {
        index_block_handle_ = index_block_handle;
    }

    BlockHandle GetIndexHandle() const { return index_block_handle_; }

    void SetFilterHandle(const BlockHandle& filter_index_handle) {
        filter_index_handle_ = filter_index_handle;
    }

    BlockHandle GetFilterHandle() const { return filter_index_handle_; }

    void EncodeTo(std::string* dst);

    Status DecodeFrom(std::string_view* input);
 private:
    BlockHandle index_block_handle_;
    BlockHandle filter_index_handle_;
};

Status ReadBlock(const ReadOption& opt, RandomReadFile* file, 
    const BlockHandle& handle, BlockContents* result);
}

#endif // STORAGE_XDB_DB_SSTABLE_BLOCK_FORMAT_H_