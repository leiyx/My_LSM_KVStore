#include "db/filter/filter_block.h"

#include <cassert>

#include "util/coding.h"
namespace lsmkv {

// every FilterBlock has 2KB of data
static const size_t KFilterBlockSizeLg = 11;

static const size_t KFilterBlockSize = (1 << KFilterBlockSizeLg);

FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy)
    : policy_(policy) {}
void FilterBlockBuilder::StartBlock(uint64_t block_offset) {
  size_t filter_index = block_offset / KFilterBlockSize;
  assert(filter_index >= filter_offsets_.size());
  while (filter_index > filter_offsets_.size()) {
    GenerateFilter();
  }
}

void FilterBlockBuilder::AddKey(std::string_view key) {
  key_starts_.push_back(key_buffer_.size());
  key_buffer_.append(key.data(), key.size());
}

std::string_view FilterBlockBuilder::Finish() {
  if (!key_starts_.empty()) {
    GenerateFilter();
  }
  const uint32_t filter_offsets_start = result_.size();
  for (size_t i = 0; i < filter_offsets_.size(); i++) {
    PutFixed32(&result_, filter_offsets_[i]);
  }
  PutFixed32(&result_, filter_offsets_start);
  result_.push_back(static_cast<char>(KFilterBlockSizeLg));
  return std::string_view(result_);
}

void FilterBlockBuilder::GenerateFilter() {
  size_t key_num = key_starts_.size();
  if (key_num == 0) {
    filter_offsets_.push_back(result_.size());
    return;
  }
  key_starts_.push_back(key_buffer_.size());
  tmp_keys_.resize(key_num);
  for (size_t i = 0; i < key_num; i++) {
    const char* p = key_buffer_.data() + key_starts_[i];
    const size_t len = key_starts_[i + 1] - key_starts_[i];
    tmp_keys_[i] = std::string_view(p, len);
  }
  filter_offsets_.push_back(result_.size());
  policy_->CreatFilter(&tmp_keys_[0], static_cast<int>(key_num), &result_);

  tmp_keys_.clear();
  key_starts_.clear();
  key_buffer_.clear();
}

FilterBlockReader::FilterBlockReader(const FilterPolicy* policy,
                                     std::string_view contents)
    : policy_(policy),
      data_(nullptr),
      filter_offsets_start_(nullptr),
      filter_offsets_num_(0),
      filter_block_size_length_(0) {
  size_t n = contents.size();
  if (n < 5) return;
  filter_block_size_length_ = contents[n - 1];
  uint32_t filter_offsets_start = DecodeFixed32(contents.data() + n - 5);
  if (filter_offsets_start > n - 5) return;
  data_ = contents.data();
  filter_offsets_start_ = data_ + filter_offsets_start;
  filter_offsets_num_ = (n - 5 - filter_offsets_start) / 4;
}

bool FilterBlockReader::KeyMayMatch(uint64_t block_offset,
                                    std::string_view key) {
  size_t index = block_offset >> filter_block_size_length_;
  if (index < filter_offsets_num_) {
    uint32_t start = DecodeFixed32(filter_offsets_start_ + index * 4);
    uint32_t limit = DecodeFixed32(filter_offsets_start_ + index * 4 + 4);
    if (start < limit &&
        limit <= static_cast<size_t>(filter_offsets_start_ - data_)) {
      std::string_view filter(data_ + start, limit - start);
      return policy_->KeyMayMatch(key, filter);
    } else if (start == limit) {
      return false;
    }
  }
  return true;
}

}  // namespace lsmkv