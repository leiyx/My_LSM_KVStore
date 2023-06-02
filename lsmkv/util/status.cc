#include "include/status.h"

#include <cassert>
#include <cstdio>
#include <cstring>
#include <string>
namespace lsmkv {

Status::Status(Code code, std::string_view msg1, std::string_view msg2) {
  assert(code != KOK);
  const uint32_t len1 = static_cast<uint32_t>(msg1.size());
  const uint32_t len2 = static_cast<uint32_t>(msg2.size());
  const uint32_t len = len1 + (len2 == 0 ? 0 : len2 + 2);
  char* result = new char[len + 5];
  std::memcpy(result, &len, sizeof(len));
  result[4] = static_cast<char>(code);
  std::memcpy(result + 5, msg1.data(), len1);
  if (len2 != 0) {
    result[5 + len1] = ':';
    result[6 + len1] = ' ';
    std::memcpy(result + 7 + len1, msg2.data(), len2);
  }
  state_ = result;
}

const char* CopyState(const char* state) {
  uint32_t len;
  std::memcpy(&len, state, sizeof(len));
  char* result = new char[len + 5];
  std::memcpy(result, state, len + 5);
  return result;
}

std::string Status::ToString() const {
  if (state_ == nullptr) {
    return "OK";
  } else {
    char tmp[30];
    const char* type;
    switch (code()) {
      case KOK:
        type = "OK";
        break;
      case KNotFound:
        type = "NotFound: ";
        break;
      case KIOError:
        type = "IO error: ";
        break;
      case KCorruption:
        type = "Corruption: ";
        break;
      default:
        std::snprintf(tmp, sizeof(tmp),
                      "Unknown(%d): ", static_cast<Code>(code()));
        type = tmp;
        break;
    }
    std::string ret(type);
    uint32_t len;
    std::memcpy(&len, state_, sizeof(len));
    ret.append(state_ + 5, len);
    return ret;
  }
}

}  // namespace lsmkv