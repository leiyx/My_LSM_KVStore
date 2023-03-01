#include "table_cache.h"

#include <fstream>
TableCache::TableCache(const std::string& file_name) {
  sst_path_ = file_name;
  Open();
}

std::string TableCache::GetValue(uint64_t key) const {
  // 通过SST文件的键值范围判断key是否存在
  if (key < min_max_key_[0] || key > min_max_key_[1]) return "";

  // 通过布隆过滤器判断key是否存在，如果有其中一个bit为0，则证明不存在
  unsigned int hash[4] = {0};
  MurmurHash3_x64_128(&key, sizeof(key), 1, hash);
  for (unsigned int i : hash)
    if (!bloom_filter_[i % 81920]) return "";

  // 在索引区中进行查找
  if (key_offset_map_.count(key) == 0) return "";
  auto iter1 = key_offset_map_.find(key);
  auto iter2 = key_offset_map_.find(key);
  iter2++;

  auto file = std::fstream(sst_path_, std::ios::in | std::ios::binary);
  file.seekg(0, std::ios::end);
  uint64_t len = iter2 != key_offset_map_.end()
                     ? iter2->second - iter1->second
                     : (int)file.tellg() - iter1->second;

  file.seekg(iter1->second);
  // file.seekg(iter1->second);
  // char* ans = new char[len];
  // file.read(ans, sizeof(char) * len);
  // file.close();
  // return ans;

  std::string ans(len, ' ');
  file.read(&(*ans.begin()), sizeof(char) * len);
  file.close();
  return ans.c_str();
  // return ans;
}

void TableCache::Open() {
  auto file = std::fstream(sst_path_, std::ios::in | std::ios::binary);
  file.read((char*)(&time_and_size_), 2 * sizeof(uint64_t));
  file.read((char*)(&min_max_key_), 2 * sizeof(int64_t));
  file.read((char*)(&bloom_filter_), sizeof(bloom_filter_));

  int size = time_and_size_[1];
  int64_t temp_key;
  uint32_t temp_key_offset;
  while (size--) {
    file.read((char*)(&temp_key), sizeof(int64_t));
    file.read((char*)(&temp_key_offset), sizeof(uint32_t));
    key_offset_map_[temp_key] = temp_key_offset;
  }
  file.close();
}

void TableCache::Traverse(std::map<int64_t, std::string>& pair) const {
  auto file = std::fstream(sst_path_, std::ios::in | std::ios::binary);
  auto iter1 = key_offset_map_.begin();
  auto iter2 = iter1;
  iter2++;

  char* ans;
  uint64_t len;
  while (iter1 != key_offset_map_.end()) {
    if (iter2 != key_offset_map_.end()) {
      len = iter2->second - iter1->second;
      iter2++;
    } else {
      file.seekg(0, std::ios::end);
      len = (int)file.tellg() - iter1->second;
    }
    file.seekg(iter1->second);

    std::string ans(len, ' ');
    file.read(&(*ans.begin()), sizeof(char) * len);
    // ans = new char[len];
    // file.read(ans, sizeof(char) * len);
    pair[iter1->first] = ans;
    iter1++;
  }
  file.close();
}