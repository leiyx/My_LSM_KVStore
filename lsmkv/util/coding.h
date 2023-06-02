#ifndef STORAGE_XDB_UTIL_CODING_H_
#define STORAGE_XDB_UTIL_CODING_H_

#include <string>

#include <string_view>

// encoding 32bits or 64bits into string.
// varint or fixed
#include <cstdint>
namespace lsmkv {

size_t VarintLength(uint64_t v);

void PutVarint32(std::string* dst, uint32_t val);
void PutVarint64(std::string* dst, uint64_t val);
void PutFixed32(std::string* dst, uint32_t val);
void PutFixed64(std::string* dst, uint64_t val);

bool GetVarint32(std::string_view* input, uint32_t* value);
bool GetVarint64(std::string_view* input, uint64_t* value);
bool GetLengthPrefixedSlice(std::string_view* input, std::string_view* result);
void PutLengthPrefixedSlice(std::string* dst, std::string_view input);

void EncodeFixed32(char* dst, uint32_t val);
void EncodeFixed64(char* dst, uint64_t val);
char* EncodeVarint32(char* dst, uint32_t val);
char* EncodeVarint64(char* dst, uint64_t val);

const char* DecodeVarint32(const char* dst, const char* limit,
                           uint32_t* result);
const char* DecodeVarint64(const char* dst, const char* limit,
                           uint64_t* result);
uint32_t DecodeFixed32(const char* dst);
uint64_t DecodeFixed64(const char* dst);

inline void EncodeFixed32(char* dst, uint32_t val) {
  uint8_t* buf = reinterpret_cast<uint8_t*>(dst);

  buf[0] = static_cast<uint8_t>(val);
  buf[1] = static_cast<uint8_t>(val >> 8);
  buf[2] = static_cast<uint8_t>(val >> 16);
  buf[3] = static_cast<uint8_t>(val >> 24);
}

inline void EncodeFixed64(char* dst, uint64_t val) {
  uint8_t* buf = reinterpret_cast<uint8_t*>(dst);

  buf[0] = static_cast<uint8_t>(val);
  buf[1] = static_cast<uint8_t>(val >> 8);
  buf[2] = static_cast<uint8_t>(val >> 16);
  buf[3] = static_cast<uint8_t>(val >> 24);
  buf[4] = static_cast<uint8_t>(val >> 32);
  buf[5] = static_cast<uint8_t>(val >> 40);
  buf[6] = static_cast<uint8_t>(val >> 48);
  buf[7] = static_cast<uint8_t>(val >> 56);
}

inline void PutFixed32(std::string* dst, uint32_t val) {
  char buf[sizeof(val)];
  EncodeFixed32(buf, val);
  dst->append(buf, sizeof(buf));
}

inline void PutFixed64(std::string* dst, uint64_t val) {
  char buf[sizeof(val)];
  EncodeFixed64(buf, val);
  dst->append(buf, sizeof(buf));
}

static const uint32_t KMaskValue = 0x1af289ae;

uint32_t CrcMask(uint32_t crc);

uint32_t CrcUnMask(uint32_t crc);

}  // namespace lsmkv
#endif  // STORAGE_XDB_UTIL_CODING_H_