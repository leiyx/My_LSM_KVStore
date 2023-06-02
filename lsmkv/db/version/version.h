#ifndef STORAGE_XDB_DB_VERSION_VERSION_H_
#define STORAGE_XDB_DB_VERSION_VERSION_H_

#include <cassert>
#include <iostream>

#include "db/format/dbformat.h"
#include "db/log/log_writer.h"
#include "db/sstable/table_cache.h"
#include "db/version/version_edit.h"
#include "include/comparator.h"
#include "include/iterator.h"
#include "include/option.h"
#include "util/mutex.h"

namespace lsmkv {

class VersionSet;
class Compaction;

// find first file that "largest >= internal_key",
// the files must be sorted by largest and have no overlap(level > 0)
size_t FindFile(const std::vector<FileMeta*>& files, std::string_view user_key,
                const Comparator* ucmp);

class Version {
 public:
  // 存放seek file相关信息: seek_file_level、seek_file
  struct GetStats {
    int seek_file_level;
    FileMeta* seek_file;
  };
  void Ref();
  void Unref();

  /**
   * @brief 内存中没有找到，从Version（SST 文件）中找
   * @details 由DBImpl::Get调用
   * @param[in] option 读操作选项
   * @param[in] key 查找的key
   * @param[out] result 存放Get结果
   * @param[out] stats
   * @return Status 表示执行状态
   */
  Status Get(const ReadOption& option, const LookupKey& key,
             std::string* result, GetStats* stats);

  /**
   * @brief seek SST文件一次的相应处理
   * @details DBImpl::Get中被调用
   * @param[in] stats
   * @return
   *    @retval true 触发major compaction
   *    @retval false 未触发major compaction
   */
  bool UpdateStats(const GetStats& stats);

  /**
   * @brief Get the Overlapping FileMeta
   * @details 被VersionSet::PickCompaction调用
   * @param[in] level
   * @param[in] smallest
   * @param[in] largest
   * @param[out] input 结果存放
   */
  void GetOverlappingFiles(int level, const InternalKey& smallest,
                           const InternalKey& largest,
                           std::vector<FileMeta*>* input);

 private:
  friend class VersionSet;
  friend class Compaction;
  class LevelFileIterator;
  explicit Version(VersionSet* vset)
      : vset_(vset),
        next_(this),
        prev_(this),
        refs_(0),
        file_to_compact_level_(-1),
        file_to_compact_(nullptr),
        compaction_level(-1),
        compaction_score(-1) {}

  Version(const Version&) = delete;
  Version& operator=(const Version&) = delete;

  ~Version();

  /**
   * @brief
   * @details 仅仅用于Version::Get
   * @param[in] user_key
   * @param[in] internal_key
   * @param[in] arg
   * @param[in] fun
   */
  void ForEachFile(std::string_view user_key, std::string_view internal_key,
                   void* arg, bool (*fun)(void*, int, FileMeta*));

 private:
  VersionSet* vset_;
  Version* next_;
  Version* prev_;
  int refs_;

  std::vector<FileMeta*> files_[config::kNumLevels];

  // compaction case 1: When file is seeked to many times during
  // "Version->Get", it should be compact.
  int file_to_compact_level_;
  FileMeta* file_to_compact_;

  // compaction case 2: When a level's usage is to much(file num
  // for level0, file total size for other level), compact this level.
  // the score is setted by EvalCompactionScore()
  int compaction_level;
  double compaction_score;
};

class VersionSet {
 public:
  VersionSet(const std::string name, const Option* option, TableCache* cache,
             const InternalKeyComparator* cmp);

  VersionSet(const VersionSet&) = delete;
  VersionSet& operator=(const VersionSet&) = delete;

  ~VersionSet();

  Version* Current() { return current_; }

  Status LogAndApply(VersionEdit* edit, Mutex* mu) EXCLUSIVE_LOCKS_REQUIRED(mu);

  Status Recover();

  uint64_t NextFileNumber() { return next_file_number_++; }

  uint64_t LastSequence() const { return last_sequence_; }

  uint64_t LogNumber() const { return log_number_; }

  uint64_t MetaFileNumber() const { return meta_file_number_; }

  void MarkFileNumberUsed(uint64_t number) {
    if (number >= next_file_number_) {
      next_file_number_ = number + 1;
    }
  }

  uint64_t LevelFileNum(int level) {
    assert(level >= 0 && level <= config::kNumLevels);
    return current_->files_[level].size();
  }
  void SetLastSequence(uint64_t s) {
    assert(s >= last_sequence_);
    last_sequence_ = s;
  }

  void AddLiveFiles(std::set<uint64_t>* live);

  Compaction* PickCompaction();

  Iterator* MakeMergedIterator(Compaction* c);

  bool NeedCompaction() {
    Version* v = current_;
    return (v->compaction_score >= 1) || (v->file_to_compact_ != nullptr);
  }

 private:
  class Builder;

  friend class Version;
  friend class Compaction;

  Status WriteSnapShot(log::Writer* writer);

  void AppendVersion(Version* v);

  void EvalCompactionScore(Version* v);

  void GetRange(const std::vector<FileMeta*>& input, InternalKey* smallest,
                InternalKey* largest);

  void GetTwoRange(const std::vector<FileMeta*>& input1,
                   const std::vector<FileMeta*>& input2, InternalKey* smallest,
                   InternalKey* largest);

 private:
  const std::string name_;
  const Option* option_;
  Env* env_;
  const InternalKeyComparator icmp_;

  TableCache* table_cache_;
  Version dummy_head_;
  Version* current_;

  uint64_t log_number_;
  SequenceNum last_sequence_;
  uint64_t next_file_number_;
  uint64_t meta_file_number_;

  WritableFile* meta_log_file_;
  log::Writer* meta_log_writer_;

  std::string compactor_pointer_[config::kNumLevels];
};

class Compaction {
 public:
  Compaction(const Option* option, int level);

  ~Compaction() {
    if (input_version_ != nullptr) {
      input_version_->Unref();
      input_version_ = nullptr;
    }
  }
  bool SingalMove() const;

  int level() { return level_; }

  FileMeta* input(int which, int i) { return input_[which][i]; }

  VersionEdit* edit() { return &edit_; }

  size_t InputFilesNum(int which) { return input_[which].size(); }

  bool StopBefore(std::string_view key);

  bool IsBaseLevelForKey(std::string_view key);

  uint64_t MaxOutputFileBytes() { return max_output_file_bytes_; }

  void ReleaseInput() {
    if (input_version_ != nullptr) {
      input_version_->Unref();
      input_version_ = nullptr;
    }
  }

  void AddInputDeletions(VersionEdit* edit);

  std::string InputToString(int which) {
    std::string ret{"{"};
    for (int i = 0; i < input_[which].size(); i++) {
      ret += std::to_string(input_[which][i]->number);
      if (i != input_[which].size() - 1) {
        ret += ", ";
      }
    }
    ret.push_back('}');
    return ret;
  }

 private:
  friend class Version;
  friend class VersionSet;

 private:
  int level_;
  uint64_t max_output_file_bytes_;
  std::vector<FileMeta*> input_[2];
  std::vector<FileMeta*> grandparents_;  // level_ + 1
  Version* input_version_;
  VersionEdit edit_;

  uint64_t grandparents_overlap_;
  size_t grandparents_index_;
  bool seen_key_;
};

}  // namespace lsmkv

#endif  // STORAGE_XDB_DB_VERSION_VERSION_H_