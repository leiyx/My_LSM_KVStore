#ifndef STORAGE_XDB_DB_WRITEBATCH_WRITEBATCH_HELPER_H_
#define STORAGE_XDB_DB_WRITEBATCH_WRITEBATCH_HELPER_H_

#include "include/writebatch.h"
#include "db/memtable/memtable.h"

namespace lsmkv {

constexpr const size_t KHeaderSize = 12;

class WriteBatchHelper {
 public:
    static SequenceNum GetSequenceNum(const WriteBatch *b) { return static_cast<SequenceNum>(DecodeFixed64(b->rep_.data())); }

    static void SetSequenceNum(WriteBatch *b, uint64_t val) { EncodeFixed64(&b->rep_[0],val); }

    static uint32_t GetCount(const WriteBatch *b) { return DecodeFixed32(b->rep_.data() + 8); }

    static void SetCount(WriteBatch *b, uint32_t val) { EncodeFixed32(&b->rep_[8],val); }

    static void SetContent(WriteBatch *b, std::string_view content);

    static std::string_view GetContent(WriteBatch *b);

    static void Append(WriteBatch *dst , const WriteBatch *src);

    static size_t GetSize(const WriteBatch *b) { return b->rep_.size(); }

    static Status InsertMemTable(const WriteBatch* b, MemTable* mem);
};

class MemTableInserter : public WriteBatch::Handle {
 public:
    SequenceNum seq_;
    MemTable* mem_;
    void Put(std::string_view key, std::string_view value) override {
        mem_->Put(seq_, KTypeInsertion, key, value);
        seq_++;
    }
    void Delete(std::string_view key) override {
        mem_->Put(seq_, KTypeDeletion, key, "");
        seq_++;
    }

};
}

#endif // STORAGE_XDB_DB_WRITEBATCH_WRITEBATCH_HELPER_H_