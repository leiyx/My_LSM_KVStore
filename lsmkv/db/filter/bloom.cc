#include <cstdint>

#include "include/filter_policy.h"
#include "util/MurmurHash3.h"
namespace lsmkv {

static uint32_t BloomHash(std::string_view key) {
  return murmur3::MurmurHash3_x86_32(key.data(), key.size(), 0x789fed11);
}

class BloomFilterPolicy : public FilterPolicy {
 public:
  explicit BloomFilterPolicy(int bits_per_key) : bits_per_key_(bits_per_key) {
    hash_num_ = static_cast<size_t>(bits_per_key * 0.69);
    if (hash_num_ < 1) hash_num_ = 1;
    if (hash_num_ > 30) hash_num_ = 30;
  }

  const char* Name() const override { return "lsmkv.BloomFilterPolicy"; }

  void CreatFilter(std::string_view* keys, int n,
                   std::string* dst) const override {
    int bits = n * bits_per_key_;

    if (bits < 64) bits = 64;
    size_t bytes = (bits + 7) / 8;
    bits = bytes * 8;

    const size_t old_size = dst->size();
    dst->resize(old_size + bytes, 0);
    dst->push_back(hash_num_);
    char* array = &(*dst)[old_size];
    for (int i = 0; i < n; i++) {
      uint32_t h = BloomHash(keys[i]);
      const uint32_t delta = (h >> 17) | (h << 15);
      for (size_t j = 0; j < hash_num_; j++) {
        uint32_t pos = h % bits;
        array[pos / 8] |= (1 << (pos % 8));
        h += delta;
      }
    }
  }

  bool KeyMayMatch(std::string_view key,
                   std::string_view filter) const override {
    const size_t len = filter.size();
    if (len < 2) return false;
    const char* array = filter.data();
    const size_t bits = (len - 1) * 8;
    const size_t hash_num = array[len - 1];
    if (hash_num > 30) {
      return true;
    }
    uint32_t h = BloomHash(key);
    const uint32_t delta = (h >> 17) | (h << 15);
    for (size_t i = 0; i < hash_num; i++) {
      uint32_t pos = h % bits;
      if ((array[pos / 8] & (1 << (pos % 8))) == 0) {
        return false;
      }
      h += delta;
    }
    return true;
  }

 private:
  size_t bits_per_key_;
  size_t hash_num_;
};

FilterPolicy* NewBloomFilterPolicy(int bits_per_key) {
  return new BloomFilterPolicy(bits_per_key);
}
}  // namespace lsmkv