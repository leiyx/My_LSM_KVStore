#ifndef STORAGE_XDB_INCLUDE_FILTER_POLICY_H_
#define STORAGE_XDB_INCLUDE_FILTER_POLICY_H_
#include <cstdint>

#include <string_view>
namespace lsmkv {

class FilterPolicy {
 public:
  virtual ~FilterPolicy() = default;

  virtual const char* Name() const = 0;

  // keys[0..n-1] contains a list of keys.
  // the keys is used for create the new filter.
  // and the filter should be Append to dst (encoded into string)
  virtual void CreatFilter(std::string_view* keys, int n,
                           std::string* dst) const = 0;

  // the filter is created by "CreatFilter"
  // Return true : if key is potentially contained
  // by "keys" of "CreatFilter".
  // Return false : if key must not be contained by keys.
  virtual bool KeyMayMatch(std::string_view key, std::string_view filter) const = 0;
};

FilterPolicy* NewBloomFilterPolicy(int bits_per_key);
}  // namespace lsmkv
#endif  // STORAGE_XDB_INCLUDE_FILTER_POLICY_H_