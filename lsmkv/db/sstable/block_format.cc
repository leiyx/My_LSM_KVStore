#include "crc32c/crc32c.h"
#include "db/sstable/block_format.h" 
#include "util/coding.h"
#include "include/option.h"
#include "snappy.h"

namespace lsmkv {

BlockHandle::BlockHandle() 
    : offset_(0),size_(0) {}

Status BlockHandle::DecodeFrom(std::string_view* input) {
    if(GetVarint64(input, &offset_) && GetVarint64(input, &size_)) {
        return Status::OK();
    } else {
        return Status::Corruption("bad block handle");
    }
}

void BlockHandle::EncodeTo(std::string* dst) {
    PutVarint64(dst, offset_);
    PutVarint64(dst, size_);
}

Status Footer::DecodeFrom(std::string_view* input) {
    const char* magic_ptr = input->data() + KEncodeLength - 8;
    uint64_t magic = DecodeFixed64(magic_ptr);
    if (magic != KFooterMagicNum) {
        return Status::Corruption("not a sstable");
    }
    Status s = index_block_handle_.DecodeFrom(input);
    if (s.ok()) {
        s = filter_index_handle_.DecodeFrom(input);
    }
    if (s.ok()) {
        const char* end = magic_ptr + 8;
        *input = std::string_view(end, input->data() + input->size() - end);
    }
    return s;
}

void Footer::EncodeTo(std::string* dst) {
    index_block_handle_.EncodeTo(dst);
    filter_index_handle_.EncodeTo(dst);
    dst->resize(2 * BlockHandle::KMaxEncodeLength);
    PutFixed64(dst, KFooterMagicNum);
}

Status ReadBlock(const ReadOption& option, RandomReadFile* file, 
        const BlockHandle& handle, BlockContents* result) {
    result->data = std::string_view();
    result->table_cache_ = false;
    result->heap_allocated_ = false;

    uint64_t n = handle.GetSize();
    char* buf = new char[n + KBlockTailSize];
    std::string_view contents;
    Status s = file->Read(handle.GetOffset(), n + KBlockTailSize,
            &contents, buf);
    if (contents.size() != n + KBlockTailSize) {
        delete[] buf;
        return Status::Corruption("file size is uncorrect");
    }

    const char* data = contents.data();
    if (option.check_crc) {
        const uint32_t crc = CrcUnMask(DecodeFixed32(data + n + 1));
        const uint32_t actual = crc32c::Crc32c(data, n + 1);
        if (crc != actual) {
            delete[] buf;
            return Status::Corruption("crc check mismatch");
        }
    }
    
    switch(data[n]) {
        case KUnCompress:
            if (data != buf) {
                // the mem of data is stored in other place.
                // don't need to cache and new heap buffer.
                delete[] buf;
                result->data = std::string_view(data, n);
                result->heap_allocated_ = false;
                result->table_cache_ = false;
            } else {
                result->data = std::string_view(data, n);
                result->heap_allocated_ = true;
                result->table_cache_ = true;
            }
            break;
        case KSnappyCompress: {
            size_t uncompress_len = 0;
            if (!snappy::GetUncompressedLength(data, n, &uncompress_len)) {
                delete[] buf;
                return Status::Corruption("ReadBlock: snappy GetUncompressedLength error");
            }
            char* uncompress_buf = new char[uncompress_len];
            if(!snappy::RawUncompress(data, n, uncompress_buf)) {
                delete[] buf;
                delete[] uncompress_buf;
                return Status::Corruption("ReadBlock: snappy RawUncompress error");
            }
            delete[] buf;
            result->data = std::string_view(uncompress_buf,uncompress_len);
            result->heap_allocated_ = true;
            result->table_cache_ = true;
            break;
        }
        default:
            delete[] buf;
            return Status::Corruption("ReadBlock: bad block record");
    }
    return Status::OK();
}

}