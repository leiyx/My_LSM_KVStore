#ifndef STORAGE_XDB_DB_FILTER_FILTER_BLOCK_H_
#define STORAGE_XDB_DB_FILTER_FILTER_BLOCK_H_

#include <cstdint>
#include <string>
#include <string_view>
#include <vector>

#include "include/filter_policy.h"
namespace lsmkv {

class FilterPolicy;
/**
 * @brief 用于SSTBlockBuilder
 */
class FilterBlockBuilder {
 public:
  explicit FilterBlockBuilder(const FilterPolicy* policy);

  FilterBlockBuilder(const FilterBlockBuilder&) = delete;
  FilterBlockBuilder* operator=(const FilterBlockBuilder&) = delete;

  void StartBlock(uint64_t block_offset);
  void AddKey(std::string_view key);
  std::string_view Finish();

 private:
  void GenerateFilter();

 private:
  const FilterPolicy* policy_;
  std::string key_buffer_;
  std::vector<size_t> key_starts_;
  std::string result_;
  std::vector<uint32_t> filter_offsets_;
  std::vector<std::string_view> tmp_keys_;
};

/**
 * @brief 用于SSTBlockReader
 */
class FilterBlockReader {
 public:
  FilterBlockReader(const FilterPolicy* policy, std::string_view contents);

  bool KeyMayMatch(uint64_t block_offset, std::string_view key);

 private:
  const FilterPolicy* policy_;
  const char* data_;
  const char* filter_offsets_start_;
  size_t filter_offsets_num_;
  size_t filter_block_size_length_;
};

}  // namespace lsmkv

#endif  // STORAGE_XDB_DB_FILTER_FILTER_BLOCK_H_