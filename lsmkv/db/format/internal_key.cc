#include "db/format/internal_key.h"

#include <cstring>
namespace lsmkv {

void AppendInternalKey(std::string* dst, const ParsedInternalKey& key) {
  dst->append(key.user_key_.data(), key.user_key_.size());
  ;
  PutFixed64(dst, PackSequenceAndType(key.seq_, key.type_));
}

bool ParseInternalKey(std::string_view internal_key,
                      ParsedInternalKey* result) {
  const size_t n = internal_key.size();
  if (n < 8) {
    return false;
  }
  uint64_t num = DecodeFixed64(internal_key.data() + n - 8);
  uint8_t type = num & 0xff;
  result->seq_ = num >> 8;
  result->type_ = static_cast<RecordType>(type);
  result->user_key_ = std::string_view(internal_key.data(), n - 8);
  return (type <= static_cast<uint8_t>(KTypeInsertion));
}

LookupKey::LookupKey(std::string_view user_key, SequenceNum seq) {
  size_t key_size = user_key.size();
  size_t need = key_size + 13;
  if (need < sizeof(buf_)) {
    start_ = buf_;
  } else {
    start_ = new char[need];
  }
  kstart_ = EncodeVarint32(start_, key_size + 8);
  std::memcpy(kstart_, user_key.data(), key_size);
  EncodeFixed64(kstart_ + key_size, (seq << 8) | KTypeLookup);
  end_ = kstart_ + key_size + 8;
}

LookupKey::~LookupKey() {
  if (start_ != buf_) {
    delete[] start_;
  }
}

int InternalKeyComparator::Compare(std::string_view a,
                                   std::string_view b) const {
  int r = user_cmp_->Compare(ExtractUserKey(a), ExtractUserKey(b));
  if (r == 0) {
    // num is Sequence | RecodeType
    const uint64_t anum = DecodeFixed64(a.data() + a.size() - 8);
    const uint64_t bnum = DecodeFixed64(b.data() + b.size() - 8);
    if (anum > bnum) {
      r = -1;
    } else if (anum < bnum) {
      r = +1;
    }
  }
  return r;
}

void InternalKeyComparator::FindShortestMiddle(std::string* start,
                                               std::string_view limit) const {
  std::string_view start_user = ExtractUserKey(*start);
  std::string_view limit_user = ExtractUserKey(limit);
  std::string tmp(start_user.data(), start_user.size());
  user_cmp_->FindShortestMiddle(&tmp, limit_user);
  if (tmp.size() < start_user.size() &&
      user_cmp_->Compare(start_user, tmp) < 0) {
    PutFixed64(&tmp, PackSequenceAndType(KMaxSequenceNum, KTypeLookup));
    start->swap(tmp);
  }
}

void InternalKeyComparator::FindShortestBigger(std::string* start) const {
  std::string_view start_user = ExtractUserKey(*start);
  std::string tmp(start->data(), start->size());
  user_cmp_->FindShortestBigger(&tmp);
  if (tmp.size() < start_user.size() &&
      user_cmp_->Compare(start_user, tmp) < 0) {
    PutFixed64(&tmp, PackSequenceAndType(KMaxSequenceNum, KTypeLookup));
    start->swap(tmp);
  }
}

const char* InteralKeyFilterPolicy::Name() const {
  return user_policy_->Name();
}

void InteralKeyFilterPolicy::CreatFilter(std::string_view* keys, int n,
                                         std::string* dst) const {
  std::string_view* user_keys = const_cast<std::string_view*>(keys);
  for (int i = 0; i < n; i++) {
    user_keys[i] = ExtractUserKey(keys[i]);
  }
  user_policy_->CreatFilter(user_keys, n, dst);
}

bool InteralKeyFilterPolicy::KeyMayMatch(std::string_view key,
                                         std::string_view filter) const {
  return user_policy_->KeyMayMatch(ExtractUserKey(key), filter);
}

}  // namespace lsmkv