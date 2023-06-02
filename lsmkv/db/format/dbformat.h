#ifndef DBFORMAT_H
#define DBFORMAT_H
#include <cstdint>
namespace lsmkv {

namespace config {
static constexpr int kNumLevels = 7;

static constexpr int kL0CompactionThreshold = 4;

static constexpr int kL0StopWriteThreshold = 12;

static constexpr uint64_t kMaxSequenceNumber = ((0x1ull << 56) - 1);
}  // namespace config

}  // namespace lsmkv

#endif  // DBFORMAT_H
