#ifndef STORAGE_XDB_DB_INCLUDE_COMPARATOR_H_
#define STORAGE_XDB_DB_INCLUDE_COMPARATOR_H_

#include <cstdint>

#include <string_view>
namespace lsmkv {
class Comparator {
 public:
  virtual ~Comparator() = default;

  virtual int Compare(std::string_view a, std::string_view b) const = 0;

  virtual const char* Name() const = 0;
  // find the shortest string between with start and limit.
  virtual void FindShortestMiddle(std::string* start,
                                  std::string_view limit) const = 0;

  virtual void FindShortestBigger(std::string* start) const = 0;
};

const Comparator* DefaultComparator();

}  // namespace lsmkv

#endif  // STORAGE_XDB_DB_INCLUDE_COMPARATOR_H_