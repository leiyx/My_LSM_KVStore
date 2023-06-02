#include "db/memtable/memtable.h"

#include <cstring>

#include "include/iterator.h"
#include "util/coding.h"
namespace lsmkv {

static std::string_view DecodeLengthPrefixedSlice(const char* p) {
  uint32_t len;
  const char* q = DecodeVarint32(p, p + 5, &len);
  return std::string_view(q, len);
}

static const char* MakeKey(std::string* buf, std::string_view s) {
  buf->clear();
  PutVarint32(buf, s.size());
  buf->append(s.data(), s.size());
  return buf->data();
}

int MemTable::KeyComparator::operator()(const char* a, const char* b) const {
  std::string_view as = DecodeLengthPrefixedSlice(a);
  std::string_view bs = DecodeLengthPrefixedSlice(b);
  return comparator.Compare(as, bs);
}

MemTable::MemTable(const InternalKeyComparator& cmp)
    : comparator_(cmp), table_(comparator_, &arena_), refs_(0) {}

// Format :
// Varint32 : key size + 8.
// char[key size] : key
// seq and type : Sequence | RecodeType
// Varint32 : value size.
// char[value size] : value
void MemTable::Put(SequenceNum seq, RecordType type, std::string_view key,
                   std::string_view value) {
  uint64_t seq_and_type = PackSequenceAndType(seq, type);
  size_t key_size = key.size();
  size_t val_size = value.size();
  size_t internal_key_size = key_size + 8;
  size_t total_size = VarintLength(internal_key_size) + internal_key_size +
                      VarintLength(val_size) + val_size;
  char* buf = arena_.Allocate(total_size);
  char* p = EncodeVarint32(buf, internal_key_size);
  std::memcpy(p, key.data(), key_size);
  p += key_size;
  EncodeFixed64(p, seq_and_type);
  p += 8;
  p = EncodeVarint32(p, val_size);
  std::memcpy(p, value.data(), val_size);
  table_.Insert(buf);
}

bool MemTable::Get(const LookupKey& key, std::string* result, Status* status) {
  Table::Iterator iter(&table_);
  std::string_view full_key = key.FullKey();
  iter.Seek(full_key.data());

  if (iter.Valid()) {
    const char* record = iter.key();
    uint32_t key_size;
    const char* key_ptr = DecodeVarint32(record, record + 5, &key_size);
    if (comparator_.comparator.UserComparator()->Compare(
            key.UserKey(), std::string_view(key_ptr, key_size - 8)) == 0) {
      uint64_t seq_and_type = DecodeFixed64(key_ptr + key_size - 8);
      switch (static_cast<RecordType>(seq_and_type & 0xff)) {
        case KTypeInsertion: {
          std::string_view val = DecodeLengthPrefixedSlice(key_ptr + key_size);
          result->assign(val.data(), val.size());
          return true;
        }
        case KTypeDeletion:
          *status = Status::NotFound(std::string_view());
          return true;
      }
    }
  }
  return false;
}

class MemTableIterator : public Iterator {
 public:
  explicit MemTableIterator(MemTable::Table* table) : iter_(table) {}

  MemTableIterator(const MemTableIterator&) = delete;
  MemTableIterator& operator=(const MemTableIterator&) = delete;

  ~MemTableIterator() = default;

  bool Valid() const { return iter_.Valid(); }

  std::string_view Key() const {
    return DecodeLengthPrefixedSlice(iter_.key());
  }

  std::string_view Value() const {
    std::string_view key = DecodeLengthPrefixedSlice(iter_.key());
    return DecodeLengthPrefixedSlice(key.data() + key.size());
  }

  void Next() { iter_.Next(); }

  void Prev() { iter_.Prev(); }

  void Seek(std::string_view key) { iter_.Seek(MakeKey(&alloc_ptr_, key)); }

  void SeekToFirst() { iter_.SeekToFirst(); }

  void SeekToLast() { iter_.SeekToLast(); }

  Status status() { return Status::OK(); }

 private:
  MemTable::Table::Iterator iter_;
  std::string alloc_ptr_;
};

Iterator* MemTable::NewIterator() { return new MemTableIterator(&table_); }
}  // namespace lsmkv