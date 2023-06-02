#include "db/version/version_edit.h"

#include "db/format/dbformat.h"
#include "include/status.h"

namespace lsmkv {

enum Tag {
  KLogNumber = 1,
  KLastSequence = 2,
  KNextFileNumber = 3,
  KComparatorName = 4,
  KNewFiles = 5,
  KDeleteFiles = 6,
  KCompactionPointers = 7,
};

void VersionEdit::EncodeTo(std::string* dst) {
  if (has_log_number_) {
    PutVarint32(dst, KLogNumber);
    PutVarint64(dst, log_number_);
  }
  if (has_last_sequence_) {
    PutVarint32(dst, KLastSequence);
    PutVarint64(dst, last_sequence_);
  }
  if (has_next_file_number_) {
    PutVarint32(dst, KNextFileNumber);
    PutVarint64(dst, next_file_number_);
  }
  if (has_comparator_name_) {
    PutVarint32(dst, KComparatorName);
    PutLengthPrefixedSlice(dst, comparator_name_);
  }
  for (size_t i = 0; i < new_files_.size(); i++) {
    const FileMeta& meta = new_files_[i].second;
    PutVarint32(dst, KNewFiles);
    PutVarint32(dst, new_files_[i].first);
    PutVarint64(dst, meta.number);
    PutVarint64(dst, meta.file_size);
    PutLengthPrefixedSlice(dst, meta.smallest.Encode());
    PutLengthPrefixedSlice(dst, meta.largest.Encode());
  }
  for (const auto& delete_file : delete_files_) {
    PutVarint32(dst, KDeleteFiles);
    PutVarint32(dst, delete_file.first);
    PutVarint64(dst, delete_file.second);
  }
  for (const auto& compaction_pointer : compaction_pointers_) {
    PutVarint32(dst, KCompactionPointers);
    PutVarint32(dst, compaction_pointer.first);
    PutLengthPrefixedSlice(dst, compaction_pointer.second.Encode());
  }
}

bool GetLevel(std::string_view* input, int* value) {
  uint32_t v;
  if (GetVarint32(input, &v) && v < config::kNumLevels) {
    *value = v;
    return true;
  }
  return false;
}

bool GetInternalKey(std::string_view* input, InternalKey* key) {
  std::string_view str;
  if (GetLengthPrefixedSlice(input, &str)) {
    key->DecodeFrom(str);
    return true;
  }
  return false;
}

Status VersionEdit::DecodeFrom(std::string_view src) {
  std::string_view input = src;
  int level;
  uint32_t tag;
  uint64_t number;
  std::string_view str;
  FileMeta meta;
  InternalKey key;

  while (GetVarint32(&input, &tag)) {
    switch (tag) {
      case KComparatorName:
        if (!GetLengthPrefixedSlice(&input, &str)) {
          return Status::Corruption("VersionEdit DecodeFrom: comparator");
        }
        comparator_name_ = str;
        has_comparator_name_ = true;
        break;
      case KLastSequence:
        if (!GetVarint64(&input, &number)) {
          return Status::Corruption("VersionEdit DecodeFrom: last_sequence");
        }
        last_sequence_ = number;
        has_last_sequence_ = true;
        break;
      case KLogNumber:
        if (!GetVarint64(&input, &number)) {
          return Status::Corruption("VersionEdit DecodeFrom: log_number");
        }
        log_number_ = number;
        has_log_number_ = true;
        break;
      case KNextFileNumber:
        if (!GetVarint64(&input, &number)) {
          return Status::Corruption("VersionEdit DecodeFrom: next_file_number");
        }
        next_file_number_ = number;
        has_next_file_number_ = true;
        break;
      case KNewFiles:
        if (!GetLevel(&input, &level) || !GetVarint64(&input, &meta.number) ||
            !GetVarint64(&input, &meta.file_size) ||
            !GetInternalKey(&input, &meta.smallest) ||
            !GetInternalKey(&input, &meta.largest)) {
          return Status::Corruption(
              "VersionEdit DecodeFrom: "
              "new_fhttps://github.com/ByteTech-7355608/douyin-server/pull/"
              "64iles");
        }
        new_files_.emplace_back(level, meta);
        break;
      case KDeleteFiles:
        if (!GetLevel(&input, &level) || !GetVarint64(&input, &number)) {
          return Status::Corruption("VersionEdit DecodeFrom: delete_files");
        }
        delete_files_.emplace(level, number);
        break;
      case KCompactionPointers:
        if (!GetLevel(&input, &level) || !GetInternalKey(&input, &key)) {
          return Status::Corruption(
              "VersionEdit DecodeFrom: compaction_pointer");
        }
        compaction_pointers_.emplace_back(level, key);
        break;
      default:
        return Status::Corruption("VersionEdit DecodeFrom: unknown tag");
    }
  }
  return Status::OK();
}

};  // namespace lsmkv