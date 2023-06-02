#ifndef STORAGE_XDB_DB_DBIMPL_H_
#define STORAGE_XDB_DB_DBIMPL_H_

#include <deque>

#include "db/log/log_writer.h"
#include "db/memtable/memtable.h"
#include "db/sstable/table_cache.h"
#include "db/version/version.h"
#include "include/db.h"
#include "include/env.h"

namespace lsmkv {

class DBImpl : public DB {
 public:
  DBImpl(const Option& option, const std::string& name);

  ~DBImpl() override;

  Status Get(const ReadOption& option, std::string_view key,
             std::string* value) override;

  Status Put(const WriteOption& option, std::string_view key,
             std::string_view value) override;

  Status Delete(const WriteOption& option, std::string_view key) override;

  Status Write(const WriteOption& option, WriteBatch* batch) override;

 private:
  friend class DB;
  struct Writer;
  struct CompactionState;

  WriteBatch* MergeBatchGroup(Writer** last_writer);

  Status Recover(VersionEdit* edit) EXCLUSIVE_LOCKS_REQUIRED(mu_);

  Status Initialize();

  Status MakeRoomForWrite() EXCLUSIVE_LOCKS_REQUIRED(mu_);

  Status RecoverLogFile(uint64_t number, SequenceNum* max_sequence,
                        VersionEdit* edit) EXCLUSIVE_LOCKS_REQUIRED(mu_);

  Status WriteLevel0SSTable(MemTable* mem, VersionEdit* edit)
      EXCLUSIVE_LOCKS_REQUIRED(mu_);

  void MayScheduleCompaction() EXCLUSIVE_LOCKS_REQUIRED(mu_);

  static void CompactionSchedule(void* db);

  void BackgroundCompactionCall();

  void BackgroundCompaction() EXCLUSIVE_LOCKS_REQUIRED(mu_);

  void CompactionMemtable() EXCLUSIVE_LOCKS_REQUIRED(mu_);

  Status DoCompactionLevel(CompactionState* state)
      EXCLUSIVE_LOCKS_REQUIRED(mu_);

  Status LogCompactionResult(CompactionState* state)
      EXCLUSIVE_LOCKS_REQUIRED(mu_);

  void RecordBackgroundError(Status s) EXCLUSIVE_LOCKS_REQUIRED(mu_);

  void GarbageFilesClean() EXCLUSIVE_LOCKS_REQUIRED(mu_);

  Status FinishCompactionSSTable(CompactionState* state, Iterator* input);

  Status OpenCompactionSSTable(CompactionState* state);

  void CleanCompaction(CompactionState* state) EXCLUSIVE_LOCKS_REQUIRED(mu_);

  const std::string name_;
  const InternalKeyComparator internal_comparator_;
  const InteralKeyFilterPolicy internal_policy_;
  const Option option_;

  FileLock* file_lock_;
  Env* env_;
  // in memory cache and its write-ahead logger
  MemTable* mem_;
  MemTable* imm_;
  log::Writer* log_;
  WritableFile* logfile_;
  uint64_t logfile_number_ GUARDED_BY(mu_);
  SequenceNum last_seq_;
  Mutex mu_;

  CondVar background_cv_;
  Status background_status_ GUARDED_BY(mu_);
  bool background_scheduled_ GUARDED_BY(mu_);
  std::atomic<bool> closed_;
  std::atomic<bool> has_imm_;

  std::set<uint64_t> files_writing_ GUARDED_BY(mu_);

  TableCache* table_cache_;
  VersionSet* vset_;
  WriteBatch* tmp_batch_ GUARDED_BY(mu_);
  std::deque<Writer*> writers_ GUARDED_BY(mu_);
};

Option AdaptOption(const std::string& name, const InternalKeyComparator* icmp,
                   const InteralKeyFilterPolicy* ipolicy, const Option& option);
}  // namespace lsmkv

#endif  // STORAGE_XDB_DB_DBIMPL_H_