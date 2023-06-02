#include "db/log/log_reader.h"

#include <cassert>

#include "crc32c/crc32c.h"
#include "util/coding.h"
#include "util/file.h"

namespace lsmkv {
namespace log {

Reader::Reader(SequentialFile* src, bool checksum, uint64_t initial_offset)
    : src_(src),
      checksum_(checksum),
      initial_offset_(initial_offset),
      buffer_mem_(new char[kBlockSize]),
      eof_(false),
      last_record_offset_(0),
      buffer_end_offset_(0) {}
Reader::~Reader() { delete[] buffer_mem_; }
bool Reader::ReadRecord(std::string_view* record, std::string* buffer) {
  *record = "";
  buffer->clear();

  uint64_t first_fragment_offset;
  std::string_view fragment;
  while (true) {
    const unsigned int type = ReadPhysicalRecord(&fragment);
    uint64_t fragment_offset =
        buffer_end_offset_ - buffer_.size() - KLogHeadSize - fragment.size();
    switch (type) {
      case KFullType:
        buffer->clear();
        *record = fragment;
        last_record_offset_ = fragment_offset;
        return true;
      case KFirstType:
        first_fragment_offset = fragment_offset;
        buffer->assign(fragment.data(), fragment.size());
        break;
      case KMiddleType:
        buffer->append(fragment.data(), fragment.size());
        break;
      case KLastType:
        buffer->append(fragment.data(), fragment.size());
        *record = std::string_view(*buffer);
        last_record_offset_ = first_fragment_offset;
        return true;
      case KBadRecord:
        // ignore the bad record.
        break;
      case KEof:
        return false;
    }
  }
  return false;
}

unsigned int Reader::ReadPhysicalRecord(std::string_view* fragments) {
  while (true) {
    if (buffer_.size() < KLogHeadSize) {
      if (eof_) {
        buffer_ = "";
        return KEof;
      } else {
        buffer_ = "";
        Status s = src_->Read(kBlockSize, &buffer_, buffer_mem_);
        if (!s.ok()) {
          buffer_ = "";
          eof_ = true;
          return KEof;
        }
        if (buffer_.size() < kBlockSize) {
          eof_ = true;
        }
        continue;
      }
    }

    const char* header = buffer_.data();
    const uint32_t length_lo = static_cast<uint32_t>(header[4]) & 0xff;
    const uint32_t length_hi = static_cast<uint32_t>(header[5]) & 0xff;
    const unsigned int type = static_cast<unsigned int>(header[6]);
    const uint32_t length = length_lo | (length_hi << 8);

    if (KLogHeadSize + length > buffer_.size()) {
      buffer_ = "";
      if (!eof_) {
        return KBadRecord;
      }
      return KEof;
    }

    if (checksum_) {
      uint32_t record_crc = CrcUnMask(DecodeFixed32(header));
      uint32_t expect_crc = crc32c::Crc32c(header + 6, length + 1);
      if (record_crc != expect_crc) {
        buffer_ = "";
        return KBadRecord;
      }
    }
    buffer_.remove_prefix(KLogHeadSize + length);
    // read the record befor initial, just ignore this record
    if (buffer_end_offset_ - buffer_.size() - KLogHeadSize - length <
        initial_offset_) {
      *fragments = "";
      return KBadRecord;
    }
    *fragments = std::string_view(header + KLogHeadSize, length);
    return type;
  }
}

};  // namespace log

}  // namespace lsmkv
