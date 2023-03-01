#include "kvstore.h"

#include <thread>

#include "utils.h"

static int GetFileNum(const std::string &file_name) {
  auto iter = file_name.find('e');
  iter++;
  std::string file = file_name.substr(iter);
  return std::stoi(file);
}

// 将dir目录下的所有SST文件的元信息缓存到sstable_meta_info_中
// 填充level_num_vec_信息
KVStore::KVStore(const std::string &dir)
    : KVStoreAPI(dir), cache_(options::kCacheCap) {
  mem_table_ = std::make_shared<SkipList>();
  dir_ = dir;
  level_num_vec_.push_back(0);
  kv_store_mode_ = normal;
  Reset();

  std::vector<std::string> ret;
  int num = utils::ScanDir(dir, ret);
  // 遍历处理0到num-1,共num层SST文件
  for (int i = 0; i < num; ++i) {
    sstable_meta_info_.emplace_back();
    std::string path = dir + "/" + ret[i];
    std::vector<std::string> file;
    int num_of_sst = utils::ScanDir(path, file);
    if (num_of_sst > 0)
      level_num_vec_.push_back(GetFileNum(file[file.size() - 1]));
    else
      level_num_vec_.push_back(0);
    // 缓存第i层各个SST文件的元信息
    for (int j = 0; j < num_of_sst; ++j) {
      std::string file_name = path + "/" + file[j];
      TableCache tc(file_name);
      sstable_meta_info_[i].insert(std::move(tc));
    }
  }
  if (sstable_meta_info_.empty()) sstable_meta_info_.emplace_back();
}

// 把内存中的两个跳表，dump到L0层。如果L0层SST文件数量超过限制，则触发Compaction
KVStore::~KVStore() {
  std::unique_lock<std::mutex> lk(m_);
  cv_.wait(lk, [&] { return kv_store_mode_ == normal; });
  kv_store_mode_ = exits;
  std::string path = dir_ + "/level0";
  mem_table_->Store(++level_num_vec_[0], path);
  if (level_num_vec_[0] == 3) MajorCompaction(1);
}

void KVStore::Put(uint64_t key, const std::string &val, bool to_cache) {
  std::unique_lock<std::shared_mutex> lock(read_write_m_);
  std::string t_val = mem_table_->Get(key);
  if (!t_val.empty())
    mem_table_->memory_ += val.size() - t_val.size();
  else
    mem_table_->memory_ +=
        4 + 8 + val.size() + 1;  // 索引值 + key + value所占的内存大小 + "\0"

  // mem_table_  -> immutable_table_的情况
  if (mem_table_->memory_ > options::kMemTable) {
    if (immutable_table_ != nullptr) {
      std::unique_lock<std::mutex> lk(m_);
      cv_.wait(lk, [&] { return kv_store_mode_ == normal; });
    }

    immutable_table_ = mem_table_;
    mem_table_ = std::make_shared<SkipList>();

    std::thread compact_thread(&KVStore::MinorCompaction, this);
    compact_thread.detach();
    mem_table_->memory_ += 4 + 8 + val.size() + 1;
  }
  mem_table_->Put(key, val);
  if (to_cache) cache_.Put(key, val);
}

void KVStore::PutTask(uint64_t key, const std::string &val, bool to_cache) {
  pool_.Enqueue(&KVStore::Put, this, key, val, to_cache);
}

std::string KVStore::Get(uint64_t key) {
  std::shared_lock<std::shared_mutex> lock(read_write_m_);
  // 先查cache_
  if (cache_.Cached(key)) return cache_.Get(key);

  // 先查mem_table_
  std::string ans = mem_table_->Get(key);
  if (!ans.empty()) {
    if (ans == options::kDelSign)
      return "";
    else
      return ans;
  }
  // 再查immutable_table_
  if (immutable_table_ != nullptr) {
    // 增加引用计数，避免被析构
    auto ptr = immutable_table_;
    ans = ptr->Get(key);
    if (!ans.empty()) {
      if (ans == options::kDelSign)
        return "";
      else
        return ans;
    }
    while (kv_store_mode_ == compact) {
      std::unique_lock<std::mutex> lk(m_);
      cv_.wait(lk, [&] { return kv_store_mode_ == normal; });
    }
  }

  // 查SST文件，从最上面的0层查到最下面一层，每层从前向后查（先查新的）。查到了就直接返回，不查询剩余的SST文件了
  // table_list表示一层SST文件的元信息
  for (const auto &table_list : sstable_meta_info_) {
    // table为一层中的一个SST文件的元信息
    for (auto table = table_list.rbegin(); table != table_list.rend();
         ++table) {
      ans = table->GetValue(key);
      if (!ans.empty()) {
        if (ans == options::kDelSign)
          return "";
        else
          return ans;
      }
    }
  }

  return ans;
}
std::future<std::string> KVStore::GetTask(uint64_t key) {
  return pool_.Enqueue(&KVStore::Get, this, key);
}

bool KVStore::Del(uint64_t key) {
  Put(key, options::kDelSign);
  return true;
}

void KVStore::DelTask(uint64_t key) { pool_.Enqueue(&KVStore::Del, this, key); }

void KVStore::Reset() {
  std::vector<std::string> file;
  int num_of_dir = utils::ScanDir(dir_, file);
  for (int i = 0; i < num_of_dir; ++i) {
    std::string path = dir_ + "/" + file[i];
    std::vector<std::string> ret;
    int numSST = utils::ScanDir(path, ret);
    for (int j = 0; j < numSST; ++j)
      utils::RmFile((path + "/" + ret[j]).c_str());
    utils::RmDir(path.c_str());
  }
}

static void Update(
    std::vector<std::map<int64_t, std::string>> &KVToCompact,
    std::vector<std::map<int64_t, std::string>::iterator> &KVToCompactIter,
    std::map<int64_t, int> &min_key, int index) {
  while (KVToCompactIter[index] != KVToCompact[index].end()) {
    int64_t select = KVToCompactIter[index]->first;
    if (min_key.count(select) == 0) {
      min_key[select] = index;
      break;
    } else if (index > min_key[select]) {
      int pos = min_key[select];
      min_key[select] = index;
      Update(KVToCompact, KVToCompactIter, min_key, pos);
      break;
    }
    KVToCompactIter[index]++;
  }
}

// minor compaction: 内存中Immutable MemTable -> L0 SSTable
void KVStore::MinorCompaction() {
  std::unique_lock<std::mutex> lk(m_);
  kv_store_mode_ = compact;
  std::string path = dir_ + "/level0";
  if (!utils::DirExists(path)) utils::Mkdir(path.c_str());

  level_num_vec_[0]++;
  immutable_table_->Store(level_num_vec_[0], path);
  std::string new_file =
      path + "/SSTable" + std::to_string(level_num_vec_[0]) + ".sst";
  TableCache newTable(new_file);
  sstable_meta_info_[0].insert(std::move(newTable));
  // 检查一下L0层是否需要compaction
  MajorCompaction(1);
  immutable_table_ = nullptr;

  kv_store_mode_ = normal;
  lk.unlock();
  cv_.notify_one();
}

void KVStore::MajorCompaction(int level) {
  // 递归中止条件，如果上层的文件数小于最大上限，则不进行合并
  int sst_num_for_level = sstable_meta_info_[level - 1].size();
  if (sst_num_for_level <= options::SSTMaxNumForLevel(level - 1)) {
    return;
  }

  // 需要进行compaction
  std::string path = dir_ + "/level" + std::to_string(level);
  // 若没有Level文件夹，先创建
  if (!utils::DirExists(path)) {
    utils::Mkdir(path.c_str());
    level_num_vec_.push_back(0);
    sstable_meta_info_.emplace_back();
  }

  bool last_level = false;
  if (level == sstable_meta_info_.size()) last_level = true;

  std::vector<TableCache> file_to_remove_levelminus1;  // 记录需要被删除的文件
  std::vector<TableCache> file_to_remove_level;

  // 需要被合并的SSTable
  std::priority_queue<TableCache, std::vector<TableCache>, std::greater<>>
      sort_table;

  uint64_t timestamp = 0;
  int64_t tempMin = INT64_MAX, tempMax = INT64_MIN;

  // 需要合并的文件数
  int compactNum = (level - 1 == 0) ? sst_num_for_level
                                    : sst_num_for_level -
                                          options::SSTMaxNumForLevel(level - 1);

  // 遍历Level-1中被合并的SSTable，获得时间戳和最大最小键
  auto it = sstable_meta_info_[level - 1].begin();
  for (int i = 0; i < compactNum; ++i) {
    TableCache table = *it;
    sort_table.push(table);
    file_to_remove_levelminus1.push_back(table);
    it++;

    timestamp = table.GetTimestamp();
    if (table.GetMaxKey() > tempMax) tempMax = table.GetMaxKey();
    if (table.GetMinKey() < tempMin) tempMin = table.GetMinKey();
  }

  // 找到Level中和Level-1中键有交集的文件
  for (auto &iter : sstable_meta_info_[level]) {
    if (iter.GetMaxKey() >= tempMin && iter.GetMinKey() <= tempMax) {
      sort_table.push(iter);
      file_to_remove_level.push_back(iter);
    }
  }

  std::vector<std::map<int64_t, std::string>>
      KVToCompact;  // 被合并的键值对，下标越大时间戳越大
  std::vector<std::map<int64_t, std::string>::iterator>
      KVToCompactIter;  // 键值对迭代器
  int size = options::kInitialSize;
  std::map<int64_t, int>
      min_key;  // minKey中只存放num个数据，分别为各个SSTable中最小键和对应的SSTable
  uint64_t temp_key;
  int index;  // 对应的SSTable序号
  std::string temp_value;
  std::map<int64_t, std::string> newTable;  // 暂存合并后的键值对

  while (!sort_table.empty()) {
    std::map<int64_t, std::string> KVPair;
    sort_table.top().Traverse(KVPair);
    sort_table.pop();
    KVToCompact.push_back(KVPair);
  }

  KVToCompactIter.resize(KVToCompact.size());
  // 获取键值对迭代器
  for (int i = KVToCompact.size() - 1; i >= 0; --i) {
    auto iter = KVToCompact[i].begin();
    while (iter != KVToCompact[i].end()) {
      if (min_key.count(iter->first) == 0)  // 如果键相同，保留时间戳较大的
      {
        min_key[iter->first] = i;
        break;
      }
      iter++;
    }
    if (iter != KVToCompact[i].end()) KVToCompactIter[i] = iter;
  }

  int nums = 0;
  // 只要minKey不为空，minKey的第一个元素一定为所有SSTable中的最小键
  // 每个循环将minKey中的最小键和对应的值加入newTable
  while (!min_key.empty()) {
    auto iter = min_key.begin();
    temp_key = iter->first;
    index = iter->second;
    temp_value = KVToCompactIter[index]->second;
    if (last_level &&
        temp_value == options::kDelSign)  // 最后一层的删除标记不写入文件
      goto next;
    size += temp_value.size() + 1 + 12;
    if (size > options::kMemTable) {
      WriteToFile(level, timestamp, newTable.size(), newTable);
      size = options::kInitialSize + temp_value.size() + 1 + 12;
    }
    newTable[temp_key] = temp_value;
  next:
    min_key.erase(temp_key);
    KVToCompactIter[index]++;
    Update(KVToCompact, KVToCompactIter, min_key, index);
    if (KVToCompactIter[index] == KVToCompact[index].end()) nums++;
  }

  // 不足2MB的文件也要写入
  if (!newTable.empty())
    WriteToFile(level, timestamp, newTable.size(), newTable);

  // 删除level和level-1中的被合并文件
  for (auto &file : file_to_remove_levelminus1) {
    utils::RmFile(file.GetFileName().data());
    sstable_meta_info_[level - 1].erase(file);
  }

  for (auto &file : file_to_remove_level) {
    utils::RmFile(file.GetFileName().data());
    sstable_meta_info_[level].erase(file);
  }

  MajorCompaction(level + 1);
}

// SST文件格式：
// 时间戳 键值对个数 min_key maxKey 布隆过滤器 索引区 数据区
void KVStore::WriteToFile(int level, uint64_t time_stamp_, uint64_t numPair,
                          std::map<int64_t, std::string> &newTable) {
  std::string path = dir_ + "/level" + std::to_string(level);
  level_num_vec_[level]++;
  std::string FileName =
      path + "/SSTable" + std::to_string(level_num_vec_[level]) + ".sst";  // ?
  std::fstream outFile(FileName, std::ios::app | std::ios::binary);

  auto iter1 = newTable.begin();
  int64_t min_key = iter1->first;
  auto iter2 = newTable.rbegin();
  int64_t maxKey = iter2->first;

  // 写入时间戳、键值对个数和最小最大键
  outFile.write((char *)(&time_stamp_), sizeof(uint64_t));
  outFile.write((char *)(&numPair), sizeof(uint64_t));
  outFile.write((char *)(&min_key), sizeof(int64_t));
  outFile.write((char *)(&maxKey), sizeof(int64_t));

  // 写入生成对应的布隆过滤器
  std::bitset<81920> filter;
  int64_t temp_key;
  const char *temp_value;
  unsigned int hash[4] = {0};
  while (iter1 != newTable.end()) {
    temp_key = iter1->first;
    MurmurHash3_x64_128(&temp_key, sizeof(temp_key), 1, hash);
    for (auto i : hash) filter.set(i % 81920);
    iter1++;
  }
  outFile.write((char *)(&filter), sizeof(filter));

  // 索引区，计算key对应的索引值
  const int dataArea = 10272 + numPair * 12;  // 数据区开始的位置
  uint32_t index = 0;
  int length = 0;
  iter1 = newTable.begin();
  while (iter1 != newTable.end()) {
    temp_key = iter1->first;
    index = dataArea + length;
    outFile.write((char *)(&temp_key), sizeof(int64_t));
    outFile.write((char *)(&index), sizeof(uint32_t));
    length += (iter1->second).size() + 1;
    iter1++;
  }

  // 数据区，存放value
  iter1 = newTable.begin();
  while (iter1 != newTable.end()) {
    temp_value = (iter1->second).c_str();
    outFile.write(temp_value, sizeof(char) * ((iter1->second).size()));
    temp_value = "\0";
    outFile.write(temp_value, sizeof(char) * 1);
    iter1++;
  }
  outFile.close();

  TableCache newSSTable(FileName);
  sstable_meta_info_[level].insert(std::move(newSSTable));

  newTable.clear();
}