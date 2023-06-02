#include "include/comparator.h"

#include <algorithm>
#include <cassert>
#include <string>
#include <utility>

namespace lsmkv {
class ByteWiseComparator : public Comparator {
 public:
  int Compare(std::string_view a, std::string_view b) const override {
    return a.compare(b);
  }
  const char* Name() const override { return "lsmkv.ByteWiseComparator"; }
  void FindShortestMiddle(std::string* start,
                          std::string_view limit) const override {
    int min_len = std::min(start->size(), limit.size());
    size_t diff_index = 0;
    while ((diff_index < min_len) &&
           (*start)[diff_index] == limit[diff_index]) {
      diff_index++;
    }
    if (diff_index < min_len) {
      uint8_t diff_byte = static_cast<uint8_t>((*start)[diff_index]);
      if (diff_byte < static_cast<uint8_t>(0xff) &&
          diff_byte + 1 < static_cast<uint8_t>(limit[diff_index])) {
        (*start)[diff_index]++;
        start->resize(diff_index + 1);
        assert(Compare(*start, limit) < 0);
      }
    }
  }
  void FindShortestBigger(std::string* start) const override {
    for (size_t i = 0; i < start->size(); i++) {
      uint8_t ch = static_cast<uint8_t>((*start)[i]);
      if (ch < static_cast<uint8_t>(0xff)) {
        (*start)[i] = ch + 1;
        start->resize(i + 1);
        return;
      }
    }
  }
};

class SingletonDefaultComparator {
 public:
  SingletonDefaultComparator() { new (storage_) ByteWiseComparator; }
  ByteWiseComparator* Get() {
    return reinterpret_cast<ByteWiseComparator*>(storage_);
  }

 private:
  alignas(ByteWiseComparator) char storage_[sizeof(ByteWiseComparator)];
};

const Comparator* DefaultComparator() {
  static SingletonDefaultComparator singleton;
  return singleton.Get();
}

}  // namespace lsmkv