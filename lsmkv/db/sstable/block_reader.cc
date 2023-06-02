#include "db/sstable/block_reader.h"

#include "db/sstable/block_format.h"
#include "util/coding.h"

namespace lsmkv {

inline size_t BlockReader::NumRestarts() const {
  assert(size_ > sizeof(uint32_t));
  return DecodeFixed32(data_ + size_ - sizeof(uint32_t));
}

BlockReader::BlockReader(const BlockContents& contents)
    : data_(contents.data.data()),
      size_(contents.data.size()),
      owned_(contents.heap_allocated_) {
  if (size_ < sizeof(uint32_t)) {
    size_ = 0;
  } else {
    size_t restarts_max = (size_ - sizeof(uint32_t)) / sizeof(uint32_t);
    if (restarts_max < NumRestarts()) {
      size_ = 0;
    } else {
      restarts_offset_ = size_ - (1 + NumRestarts()) * sizeof(uint32_t);
    }
  }
}
BlockReader::~BlockReader() {
  if (owned_) {
    delete[] data_;
  }
}
class BlockReader::Iter : public Iterator {
 public:
  Iter(const Comparator* cmp, const char* data, uint32_t restart_offset,
       uint32_t num_restarts)
      : cmp_(cmp),
        data_(data),
        num_restarts_(num_restarts),
        restart_offset_(restart_offset),
        offset_(restart_offset),
        restart_index_(num_restarts) {
    assert(num_restarts_ > 0);
  }
  bool Valid() const override { return offset_ < restart_offset_; }

  std::string_view Key() const override {
    assert(Valid());
    return key_;
  }

  std::string_view Value() const override {
    assert(Valid());
    return value_;
  }

  void Next() override {
    assert(Valid());
    SeekNextKey();
  }

  void Prev() override {
    assert(Valid());
    uint32_t old_offset = offset_;
    while (GetRestartOffset(restart_index_) >= old_offset) {
      // the entry is first entry, set Vaild() to false
      if (restart_index_ == 0) {
        offset_ = restart_offset_;
        restart_index_ = num_restarts_;
        return;
      }
      --restart_index_;
    }
    SeekToRestart(restart_index_);
    while (SeekNextKey() && NextEntryOffset() < old_offset) {
    }
  }

  void Seek(std::string_view key) override {
    uint32_t lo = 0;
    uint32_t hi = num_restarts_ - 1;

    int current_compare = 0;
    // if iter is valid, use current key to speed up.
    if (Valid()) {
      current_compare = cmp_->Compare(key_, key);
      if (current_compare < 0) {
        lo = restart_index_;
      } else if (current_compare > 0) {
        hi = restart_index_;
      } else {
        return;
      }
    }
    // binart search target:
    // find a max "lo" which key at "lo" is less than "key".
    // smaller "lo" is ok but bigger "lo" will cause uncorrect.
    while (lo < hi) {
      // in case of "lo == hi - 1", mid will be hi.
      // avoid dead loop when "lo == hi - 1 && cmp < 0".
      int mid = (lo + hi + 1) / 2;
      uint32_t restart_offset = GetRestartOffset(mid);
      uint32_t shared, non_shared, value_len;
      const char* p =
          ParsedEntry(data_ + restart_offset, data_ + restart_offset_, &shared,
                      &non_shared, &value_len);
      if (p == nullptr || shared != 0) {
        Error();
        return;
      }
      std::string_view mid_key(p, non_shared);
      if (cmp_->Compare(mid_key, key) < 0) {
        // key at "lo" is less than "key"
        // set lo to mid is safe.
        lo = mid;
      } else {
        // key at "hi" is >= than "key"
        // it is ok to set a small hi (even to -1).
        hi = mid - 1;
      }
    }
    // if iter is valid and current key is less than "key"
    // skip the seek.
    if (!(lo == restart_index_ && current_compare < 0)) {
      SeekToRestart(lo);
    }
    // linear search the first key >= "key"
    while (true) {
      if (!SeekNextKey()) {
        return;
      }
      if (cmp_->Compare(key_, key) >= 0) {
        return;
      }
    }
  }

  void SeekToFirst() override {
    SeekToRestart(0);
    SeekNextKey();
  }

  void SeekToLast() override {
    SeekToRestart(num_restarts_ - 1);
    while (SeekNextKey() && NextEntryOffset() < restart_offset_) {
    }
  }
  Status status() override { return status_; }

 private:
  uint32_t NextEntryOffset() const {
    return value_.data() + value_.size() - data_;
  }
  void Error() {
    offset_ = restart_offset_;
    restart_index_ = num_restarts_;
    status_ = Status::Corruption("bad entry in block");
    key_.clear();
    value_ = "";
  }
  const char* ParsedEntry(const char* p, const char* limit, uint32_t* shared,
                          uint32_t* non_shared, uint32_t* value_len) {
    if ((p = DecodeVarint32(p, limit, shared)) == nullptr) return nullptr;
    if ((p = DecodeVarint32(p, limit, non_shared)) == nullptr) return nullptr;
    if ((p = DecodeVarint32(p, limit, value_len)) == nullptr) return nullptr;

    if (static_cast<uint32_t>(limit - p) < (*non_shared + *value_len)) {
      return nullptr;
    }
    return p;
  }
  uint32_t GetRestartOffset(uint32_t restart_index) const {
    assert(restart_index < num_restarts_);
    return DecodeFixed32(data_ + restart_offset_ +
                         restart_index * sizeof(uint32_t));
  }
  void SeekToRestart(uint32_t restart_index) {
    key_.clear();
    restart_index_ = restart_index;
    uint32_t offset = GetRestartOffset(restart_index);
    value_ = std::string_view(data_ + offset, 0);
  }
  bool SeekNextKey() {
    offset_ = NextEntryOffset();
    const char* p = data_ + offset_;
    const char* limit = data_ + restart_offset_;
    if (p >= limit) {
      // the entry is last entry, set Vaild() to false
      offset_ = restart_offset_;
      restart_index_ = num_restarts_;
      return false;
    }
    uint32_t shared, non_shared, value_len;
    p = ParsedEntry(p, limit, &shared, &non_shared, &value_len);
    if (p == nullptr || key_.size() < shared) {
      Error();
      return false;
    }
    key_.resize(shared);
    key_.append(p, non_shared);
    value_ = std::string_view(p + non_shared, value_len);
    while (restart_index_ + 1 < num_restarts_ &&
           GetRestartOffset(restart_index_ + 1) <= offset_) {
      ++restart_index_;
    }
    return true;
  }
  const Comparator* cmp_;
  const char* data_;
  const uint32_t num_restarts_;
  const uint32_t restart_offset_;
  uint32_t offset_;
  uint32_t restart_index_;
  std::string key_;
  std::string_view value_;
  Status status_;
};

Iterator* BlockReader::NewIterator(const Comparator* cmp) {
  if (size_ < sizeof(uint32_t)) {
    return NewErrorIterator(Status::Corruption("bad block record"));
  }
  int num_restarts = NumRestarts();
  if (num_restarts == 0) {
    return NewEmptyIterator();
  }
  return new Iter(cmp, data_, restarts_offset_, num_restarts);
}

}  // namespace lsmkv