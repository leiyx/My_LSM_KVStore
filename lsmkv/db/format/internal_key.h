#ifndef INTERNAL_KEY_H
#define INTERNAL_KEY_H

#include <cassert>
#include <cstddef>
#include <string_view>

#include "include/comparator.h"
#include "include/filter_policy.h"
#include "include/status.h"
#include "util/coding.h"

namespace lsmkv {

enum RecordType {
  KTypeDeletion = 0x0,
  KTypeInsertion = 0x1,
  KTypeLookup = 0x1
};

typedef uint64_t SequenceNum;

static const SequenceNum KMaxSequenceNum = (0x1ull << 56) - 1;
static uint64_t PackSequenceAndType(SequenceNum seq, RecordType type) {
  return (seq << 8) | type;
}

inline std::string_view ExtractUserKey(std::string_view internal_key) {
  assert(internal_key.size() >= 8);
  return std::string_view(internal_key.data(), internal_key.size() - 8);
}

class ParsedInternalKey {
 public:
  ParsedInternalKey() {}
  ParsedInternalKey(SequenceNum seq, std::string_view key, RecordType type)
      : user_key_(key), seq_(seq), type_(type) {}

  std::string_view user_key_;
  SequenceNum seq_;
  RecordType type_;
};

void AppendInternalKey(std::string* dst, const ParsedInternalKey& key);

bool ParseInternalKey(std::string_view internal_key, ParsedInternalKey* result);

// Key | Sequence 56bits | Type 8bits
class InternalKey {
 public:
  InternalKey() {}
  InternalKey(SequenceNum seq, std::string_view key, RecordType type) {
    AppendInternalKey(&rep_, ParsedInternalKey(seq, key, type));
  }
  ~InternalKey() = default;

  std::string_view user_key() const { return ExtractUserKey(rep_); }

  std::string_view Encode() const { return rep_; }

  void DecodeFrom(std::string_view s) { rep_.assign(s.data(), s.size()); }

  void SetFrom(const ParsedInternalKey& key) {
    rep_.clear();
    AppendInternalKey(&rep_, key);
  }

  void Clear() { rep_.clear(); }

 private:
  std::string rep_;
};

class InternalKeyComparator : public Comparator {
 public:
  explicit InternalKeyComparator(const Comparator* c) : user_cmp_(c) {}

  const char* Name() const override { return "lsmkv.InternalKeyComparator"; }

  int Compare(std::string_view a, std::string_view b) const override;

  const Comparator* UserComparator() const { return user_cmp_; }

  int Compare(const InternalKey& a, const InternalKey& b) const {
    return Compare(a.Encode(), b.Encode());
  }

  void FindShortestMiddle(std::string* start,
                          std::string_view limit) const override;

  void FindShortestBigger(std::string* start) const override;

 private:
  const Comparator* user_cmp_;
};

class InteralKeyFilterPolicy : public FilterPolicy {
 public:
  InteralKeyFilterPolicy(const FilterPolicy* policy) : user_policy_(policy) {}
  const char* Name() const;
  void CreatFilter(std::string_view* keys, int n, std::string* dst) const;
  bool KeyMayMatch(std::string_view key, std::string_view filter) const;

 private:
  const FilterPolicy* user_policy_;
};

class LookupKey {
 public:
  LookupKey(std::string_view user_key, SequenceNum seq);

  ~LookupKey();

  LookupKey(const LookupKey&) = delete;
  LookupKey& operator=(const LookupKey&) = delete;

  std::string_view UserKey() const {
    return std::string_view(kstart_, end_ - kstart_ - 8);
  }

  std::string_view InternalKey() const {
    return std::string_view(kstart_, end_ - kstart_);
  }

  std::string_view FullKey() const {
    return std::string_view(start_, end_ - start_);
  }

 private:
  char* kstart_;
  char* end_;
  char* start_;
  char buf_[200];
};

}  // namespace lsmkv

#endif  // INTERNAL_KEY_H