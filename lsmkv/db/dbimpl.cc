#include "db/dbimpl.h"

#include <algorithm>

#include "db/log/log_reader.h"
#include "db/sstable/block_format.h"
#include "db/sstable/table_cache.h"
#include "db/writebatch/writebatch_helper.h"
#include "include/env.h"
#include "include/sstable_builder.h"
#include "util/filename.h"

namespace lsmkv {

const int KNumNonTableCache = 10;

static size_t TableCacheSize(const Option& option) {
  return option.max_open_file - KNumNonTableCache;
}

struct DBImpl::Writer {
  explicit Writer(Mutex* mu)
      : batch(nullptr), done(false), sync(false), cv(mu) {}
  Status status;
  WriteBatch* batch;
  bool done;
  bool sync;
  CondVar cv;
};

struct DBImpl::CompactionState {
  struct Output {
    uint64_t number;
    uint64_t file_size;
    InternalKey smallest;
    InternalKey largest;
  };
  explicit CompactionState(Compaction* c)
      : compaction(c), out_file(nullptr), builder(nullptr), total_bytes(0) {}

  Output* CurrOutput() { return &outputs[outputs.size() - 1]; };

  Compaction* const compaction;
  WritableFile* out_file;
  SSTableBuilder* builder;
  std::vector<Output> outputs;

  uint64_t total_bytes;
};
Option AdaptOption(const std::string& name, const InternalKeyComparator* icmp,
                   const InteralKeyFilterPolicy* ipolicy, const Option& src) {
  Option ret = src;
  ret.comparator = icmp;
  ret.filter_policy = (src.filter_policy == nullptr ? nullptr : ipolicy);
  if (ret.logger == nullptr) {
    src.env->CreatDir(name);
    Status s = src.env->NewLogger(LoggerFileName(name), &ret.logger);
    if (!s.ok()) {
      ret.logger = nullptr;
    }
  }
  return ret;
}

DBImpl::DBImpl(const Option& option, const std::string& name)
    : name_(name),
      internal_comparator_(option.comparator),
      internal_policy_(option.filter_policy),
      option_(
          AdaptOption(name, &internal_comparator_, &internal_policy_, option)),
      file_lock_(nullptr),
      env_(option.env),
      mem_(nullptr),
      imm_(nullptr),
      log_(nullptr),
      logfile_(nullptr),
      logfile_number_(0),
      last_seq_(0),
      background_cv_(&mu_),
      background_scheduled_(false),
      closed_(false),
      has_imm_(false),
      table_cache_(new TableCache(name, option_, TableCacheSize(option_))),
      vset_(
          new VersionSet(name, &option_, table_cache_, &internal_comparator_)),
      tmp_batch_(new WriteBatch) {}

DBImpl::~DBImpl() {
  mu_.Lock();
  closed_.store(true, std::memory_order_release);
  while (background_scheduled_) {
    background_cv_.Wait();
  }
  mu_.Unlock();
  if (file_lock_ != nullptr) {
    env_->UnlockFile(file_lock_);
  }
  if (mem_ != nullptr) {
    mem_->Unref();
  }
  if (imm_ != nullptr) {
    imm_->Unref();
  }
  delete vset_;
  delete tmp_batch_;
  delete log_;
  delete logfile_;
  delete table_cache_;
  delete option_.logger;
}

WriteBatch* DBImpl::MergeBatchGroup(Writer** last_writer) {
  mu_.AssertHeld();
  assert(!writers_.empty());
  Writer* first = writers_.front();
  WriteBatch* ret = first->batch;
  assert(ret != nullptr);

  size_t size = WriteBatchHelper::GetSize(ret);

  // Limit the max size. If the write is small,
  // use the lower limit to speed up the responds.
  size_t max_size = 1 << 20;
  if (size <= (1 << 10)) {
    max_size = size + (1 << 10);
  }

  *last_writer = first;
  std::deque<Writer*>::iterator iter = writers_.begin();
  ++iter;
  for (; iter != writers_.end(); ++iter) {
    Writer* w = *iter;
    if (w->sync && !first->sync) {
      break;
    }
    if (w->batch != nullptr) {
      size += WriteBatchHelper::GetSize(w->batch);
      if (size > max_size) {
        break;
      }
      if (ret == first->batch) {
        ret = tmp_batch_;
        assert(WriteBatchHelper::GetCount(ret) == 0);
        WriteBatchHelper::Append(ret, first->batch);
      }
      WriteBatchHelper::Append(ret, first->batch);
    }
    *last_writer = w;
  }
  return ret;
}

Status DBImpl::Get(const ReadOption& option, std::string_view key,
                   std::string* value) {
  Status status;
  MutexLock l(&mu_);
  SequenceNum seq = vset_->LastSequence();
  Version::GetStats stats;

  MemTable* mem = mem_;
  MemTable* imm = imm_;
  Version* current = vset_->Current();

  mem->Ref();
  if (imm != nullptr) imm->Ref();
  current->Ref();

  bool have_stats_update = false;
  {
    mu_.Unlock();
    LookupKey lkey(key, seq);
    if (mem->Get(lkey, value, &status)) {
      // found in mem
    } else if (imm != nullptr && imm->Get(lkey, value, &status)) {
      // found in imm
    } else {
      status = current->Get(option, lkey, value, &stats);
      have_stats_update = true;
    }
    mu_.Lock();
  }

  if (have_stats_update && current->UpdateStats(stats)) {
    MayScheduleCompaction();
  }

  mem->Unref();
  if (imm != nullptr) imm->Unref();
  current->Unref();
  return status;
}

Status DB::Open(const Option& option, const std::string& name, DB** ptr) {
  *ptr = nullptr;

  DBImpl* impl = new DBImpl(option, name);
  impl->mu_.Lock();
  VersionEdit edit;
  Status s = impl->Recover(&edit);
  if (s.ok()) {
    WritableFile* log_file;
    uint64_t log_number = impl->vset_->NextFileNumber();
    s = option.env->NewWritableFile(LogFileName(name, log_number), &log_file);
    if (s.ok()) {
      edit.SetLogNumber(log_number);
      impl->logfile_ = log_file;
      impl->logfile_number_ = log_number;
      impl->log_ = new log::Writer(log_file);
      impl->mem_ = new MemTable(impl->internal_comparator_);
      impl->mem_->Ref();
    }
  }
  if (s.ok()) {
    s = impl->vset_->LogAndApply(&edit, &impl->mu_);
  }
  impl->mu_.Unlock();
  if (s.ok()) {
    *ptr = impl;
  } else {
    delete impl;
  }
  return s;
}

Status DBImpl::Put(const WriteOption& option, std::string_view key,
                   std::string_view value) {
  WriteBatch batch;
  batch.Put(key, value);
  return Write(option, &batch);
}

Status DBImpl::Delete(const WriteOption& option, std::string_view key) {
  WriteBatch batch;
  batch.Delete(key);
  return Write(option, &batch);
}

Status DBImpl::Write(const WriteOption& option, WriteBatch* batch) {
  Writer w(&mu_);
  w.batch = batch;
  w.done = false;
  w.sync = option.sync;

  MutexLock l(&mu_);

  writers_.push_back(&w);
  while (!w.done && &w != writers_.front()) {
    w.cv.Wait();
  }
  if (w.done) {
    return w.status;
  }

  // merge the writebatch
  Status status;
  status = MakeRoomForWrite();
  Writer* last_writer = &w;
  SequenceNum last_seq = vset_->LastSequence();
  if (batch != nullptr) {
    WriteBatch* merged_batch = MergeBatchGroup(&last_writer);
    WriteBatchHelper::SetSequenceNum(merged_batch, last_seq + 1);
    last_seq += WriteBatchHelper::GetCount(merged_batch);
    {
      // only one thread can reach here once time
      mu_.Unlock();
      status = log_->AddRecord(WriteBatchHelper::GetContent(merged_batch));
      bool sync_error = false;
      if (status.ok() && option.sync) {
        status = logfile_->Sync();
        if (!status.ok()) {
          sync_error = true;
        }
      }
      if (status.ok()) {
        status = WriteBatchHelper::InsertMemTable(merged_batch, mem_);
      }
      mu_.Lock();
      if (sync_error) {
        RecordBackgroundError(status);
      }
    }
    if (merged_batch == tmp_batch_) {
      tmp_batch_->Clear();
    }
    vset_->SetLastSequence(last_seq);
  }
  while (true) {
    Writer* done_writer = writers_.front();
    writers_.pop_front();
    if (done_writer != &w) {
      done_writer->done = true;
      done_writer->status = status;
      done_writer->cv.Signal();
    }
    if (done_writer == last_writer) break;
  }
  if (!writers_.empty()) {
    writers_.front()->cv.Signal();
  }
  // std::cout<<"thread "<< std::this_thread::get_id() <<"done" <<std::endl;
  return status;
}

Status DBImpl::MakeRoomForWrite() {
  mu_.AssertHeld();
  Status s;
  while (true) {
    if (!background_status_.ok()) {
      s = background_status_;
      break;
    } else if (vset_->LevelFileNum(0) >= config::kL0StopWriteThreshold) {
      // a memtable is being compact as SStable
      Log(option_.logger, "Too many level-0 files. waiting...\n");
      background_cv_.Wait();
    } else if (mem_->ApproximateSize() <= option_.write_mem_size) {
      // there is enough room for write
      break;
    } else if (imm_ != nullptr) {
      // a memtable is being compact as SStable
      background_cv_.Wait();
    } else {
      uint64_t log_number = vset_->NextFileNumber();
      WritableFile* file;
      s = env_->NewWritableFile(LogFileName(name_, log_number), &file);
      if (!s.ok()) {
        break;
      }
      delete log_;
      s = logfile_->Close();
      if (!s.ok()) {
        RecordBackgroundError(s);
      }
      delete logfile_;

      logfile_ = file;
      log_ = new log::Writer(file);
      imm_ = mem_;
      mem_ = new MemTable(internal_comparator_);
      mem_->Ref();
      has_imm_.store(true, std::memory_order_release);
      MayScheduleCompaction();
    }
  }
  return s;
}

Status DBImpl::Recover(VersionEdit* edit) {
  mu_.AssertHeld();

  // Check if the DB is locked, to avoid multi-user access.
  env_->CreatDir(name_);
  Status s = env_->LockFile(LockFileName(name_), &file_lock_);
  if (!s.ok()) {
    return s;
  }
  // check if the DB has been opened
  if (!env_->FileExist(CurrentFileName(name_))) {
    // if not opened, initialize the CURRENT.
    s = Initialize();
    if (!s.ok()) {
      return Status::Corruption("DB initialize fail");
    }
  }

  s = vset_->Recover();
  if (!s.ok()) {
    return s;
  }
  uint64_t min_log = vset_->LogNumber();
  uint64_t number;
  FileType type;
  std::vector<uint64_t> log_numbers;
  std::vector<std::string> filenames;
  s = env_->GetChildren(name_, &filenames);
  if (!s.ok()) {
    return s;
  }
  for (auto& filename : filenames) {
    if (ParseFilename(filename, &number, &type)) {
      if (type == KLogFile && (number >= min_log)) {
        log_numbers.push_back(number);
      }
    }
  }
  std::sort(log_numbers.begin(), log_numbers.end());

  SequenceNum max_sequence(0);
  for (size_t i = 0; i < log_numbers.size(); i++) {
    s = RecoverLogFile(log_numbers[i], &max_sequence, edit);
    if (!s.ok()) {
      return s;
    }
    vset_->MarkFileNumberUsed(log_numbers[i]);
  }
  if (vset_->LastSequence() < max_sequence) {
    vset_->SetLastSequence(max_sequence);
  }
  return Status::OK();
}

Status DBImpl::Initialize() {
  VersionEdit edit;
  edit.SetComparatorName(internal_comparator_.UserComparator()->Name());
  edit.SetLastSequence(0);
  edit.SetLogNumber(0);
  edit.SetNextFileNumber(2);

  std::string meta_file_name = MetaFileName(name_, 1);
  WritableFile* file = nullptr;
  Status s = env_->NewWritableFile(meta_file_name, &file);
  if (!s.ok()) {
    return s;
  }

  log::Writer writer(file);
  std::string record;
  edit.EncodeTo(&record);
  s = writer.AddRecord(record);
  if (s.ok()) {
    s = file->Sync();
  }
  if (s.ok()) {
    s = file->Close();
  }
  delete file;
  if (s.ok()) {
    s = SetCurrentFile(env_, name_, 1);
  } else {
    env_->RemoveFile(meta_file_name);
  }
  return s;
}

Status DBImpl::RecoverLogFile(uint64_t number, SequenceNum* max_sequence,
                              VersionEdit* edit) {
  mu_.AssertHeld();

  std::string filename = LogFileName(name_, number);
  SequentialFile* file;
  Status s = env_->NewSequentialFile(filename, &file);
  if (!s.ok()) {
    return s;
  }
  log::Reader reader(file, true, 0);
  std::string buffer;
  std::string_view record;
  WriteBatch batch;
  MemTable* mem = nullptr;
  while (reader.ReadRecord(&record, &buffer)) {
    WriteBatchHelper::SetContent(&batch, record);
    if (mem == nullptr) {
      mem = new MemTable(internal_comparator_);
      mem->Ref();
    }
    s = WriteBatchHelper::InsertMemTable(&batch, mem);
    if (!s.ok()) {
      break;
    }
    SequenceNum last_seq = WriteBatchHelper::GetSequenceNum(&batch) +
                           WriteBatchHelper::GetCount(&batch);
    if (last_seq > *max_sequence) {
      *max_sequence = last_seq;
    }

    if (mem->ApproximateSize() > option_.write_mem_size) {
      s = WriteLevel0SSTable(mem, edit);
      mem->Unref();
      mem = nullptr;
      if (!s.ok()) {
        break;
      }
    }
  }

  if (mem != nullptr) {
    if (s.ok()) {
      s = WriteLevel0SSTable(mem, edit);
    }
    mem->Unref();
    mem = nullptr;
  }
  delete file;
  return s;
}

Status DestoryDB(const Option& option, const std::string& name) {
  Env* env = option.env;
  std::vector<std::string> filenames;
  Status s = env->GetChildren(name, &filenames);
  if (!s.ok()) {
    return s;
  }
  FileLock* lock;
  const std::string lock_filename = LockFileName(name);
  s = env->LockFile(lock_filename, &lock);
  if (!s.ok()) {
    return s;
  }
  for (auto&& filename : filenames) {
    uint64_t number;
    FileType type;
    if (ParseFilename(filename, &number, &type) && type != KLockFile) {
      Status del_s = env->RemoveFile(name + "/" + filename);
      if (!del_s.ok() && s.ok()) {
        s = del_s;
      }
    }
  }
  env->UnlockFile(lock);
  env->RemoveFile(lock_filename);
  env->RemoveDir(name);
  return s;
}

Status DBImpl::WriteLevel0SSTable(MemTable* mem, VersionEdit* edit) {
  mu_.AssertHeld();
  FileMeta meta;
  meta.number = vset_->NextFileNumber();
  files_writing_.insert(meta.number);
  Iterator* iter = mem->NewIterator();

  Log(option_.logger, "Level 0 SSTable #%llu: creating, level-0 num is %d",
      (unsigned long long)meta.number, vset_->LevelFileNum(0));

  Status s;
  {
    mu_.Unlock();
    s = BuildSSTable(name_, option_, table_cache_, iter, &meta);
    mu_.Lock();
  }
  Log(option_.logger, "Level 0 SSTable #%llu: done, level-0 num is %d",
      (unsigned long long)meta.number, vset_->LevelFileNum(0));
  delete iter;
  files_writing_.erase(meta.number);

  if (s.ok() && meta.file_size > 0) {
    edit->AddFile(0, meta.number, meta.file_size, meta.smallest, meta.largest);
  }
  return s;
}
void DBImpl::MayScheduleCompaction() {
  mu_.AssertHeld();
  if (closed_.load(std::memory_order_acquire)) {
    // DB is being deleted
  } else if (background_scheduled_) {
    // only one compaction could running
  } else if (!background_status_.ok()) {
    // compaction cause a error
  } else if (imm_ == nullptr && !vset_->NeedCompaction()) {
    // noting to do
  } else {
    background_scheduled_ = true;
    env_->Schedule(&DBImpl::CompactionSchedule, this);
  }
}

void DBImpl::CompactionSchedule(void* db) {
  reinterpret_cast<DBImpl*>(db)->BackgroundCompactionCall();
}

void DBImpl::BackgroundCompactionCall() {
  MutexLock l(&mu_);
  if (closed_.load(std::memory_order_acquire)) {
    // DB is being deleted
  } else if (!background_status_.ok()) {
    // compaction cause a error
  } else {
    BackgroundCompaction();
  }
  background_scheduled_ = false;
  MayScheduleCompaction();
  background_cv_.SignalAll();
}
Status DBImpl::DoCompactionLevel(CompactionState* state) {
  mu_.AssertHeld();

  Log(option_.logger, "Compaction SStable level-%d's %s to level-%d's %s",
      state->compaction->level(), state->compaction->InputToString(0).data(),
      state->compaction->level() + 1,
      state->compaction->InputToString(1).data());
  Iterator* input = vset_->MakeMergedIterator(state->compaction);

  mu_.Unlock();
  input->SeekToFirst();
  Status s;
  ParsedInternalKey ikey;
  std::string last_user_key;
  bool has_last_user_key = false;
  bool same_key = false;
  const Comparator* ucmp = internal_comparator_.UserComparator();
  while (input->Valid() && !closed_.load(std::memory_order_acquire)) {
    if (has_imm_.load(std::memory_order_acquire)) {
      mu_.Lock();
      if (imm_ != nullptr) {
        CompactionMemtable();
        background_cv_.SignalAll();
      }
      mu_.Unlock();
    }
    std::string_view key = input->Key();
    if (state->compaction->StopBefore(key) && state->builder != nullptr) {
      s = FinishCompactionSSTable(state, input);
      if (!s.ok()) {
        break;
      }
    }
    if (!ParseInternalKey(key, &ikey)) {
      s = Status::Corruption("DoCompactionLevel: parse key error");
      break;
    }
    if (!has_last_user_key ||
        ucmp->Compare(last_user_key, ikey.user_key_) != 0) {
      has_last_user_key = true;
      last_user_key.assign(ikey.user_key_.data(), ikey.user_key_.size());
      same_key = true;
    }
    bool drop = false;
    if (same_key) {
      // hidden the lower sequence for same key
      drop = true;
    } else if (ikey.type_ == KTypeDeletion &&
               state->compaction->IsBaseLevelForKey(ikey.user_key_)) {
      drop = true;
    }
    same_key = false;
    if (!drop) {
      if (state->builder == nullptr) {
        s = OpenCompactionSSTable(state);
        if (!s.ok()) {
          break;
        }
      }
      if (state->builder->NumEntries() == 0) {
        state->CurrOutput()->smallest.DecodeFrom(key);
      }
      state->CurrOutput()->largest.DecodeFrom(key);
      state->builder->Add(key, input->Value());
      if (state->builder->FileSize() >=
          state->compaction->MaxOutputFileBytes()) {
        s = FinishCompactionSSTable(state, input);
        if (!s.ok()) {
          break;
        }
      }
    }
    input->Next();
  }
  if (s.ok() && closed_.load(std::memory_order_acquire)) {
    s = Status::Corruption("Delete DB during compaction");
  }
  if (s.ok() && state->builder != nullptr) {
    s = FinishCompactionSSTable(state, input);
  }
  if (s.ok()) {
    s = input->status();
    if (!s.ok()) {
      Log(option_.logger, "Merged iterator err:%s", s.ToString().data());
    }
  }
  delete input;
  input = nullptr;
  mu_.Lock();

  if (s.ok()) {
    s = LogCompactionResult(state);
  }
  if (!s.ok()) {
    RecordBackgroundError(s);
  }
  return s;
}
void DBImpl::BackgroundCompaction() {
  mu_.AssertHeld();
  if (imm_ != nullptr) {
    CompactionMemtable();
    return;
  }
  Compaction* c = nullptr;
  c = vset_->PickCompaction();

  Status s;
  if (c == nullptr) {
    // nothing to do
  } else if (c->SingalMove()) {
    FileMeta* meta = c->input(0, 0);
    c->edit()->DeleteFile(c->level(), meta->number);
    c->edit()->AddFile(c->level() + 1, meta->number, meta->file_size,
                       meta->smallest, meta->largest);
    s = vset_->LogAndApply(c->edit(), &mu_);
    if (!s.ok()) {
      RecordBackgroundError(s);
    }
    Log(option_.logger, "Singal move SStable #%d level-%d to level-%d",
        meta->number, c->level(), c->level() + 1);
  } else {
    CompactionState* state = new CompactionState(c);
    s = DoCompactionLevel(state);
    if (!s.ok()) {
      RecordBackgroundError(s);
    }
    CleanCompaction(state);
    c->ReleaseInput();
    GarbageFilesClean();
  }
  delete c;
}

void DBImpl::CleanCompaction(CompactionState* state) {
  if (state->builder != nullptr) {
    delete state->builder;
  } else {
    assert(state->out_file == nullptr);
  }
  delete state->out_file;
  for (auto output : state->outputs) {
    files_writing_.erase(output.number);
  }
  delete state;
}

Status DBImpl::LogCompactionResult(CompactionState* state) {
  mu_.AssertHeld();
  Log(option_.logger, "Compaction SStable level-%d's %s to level-%d's %s OVER",
      state->compaction->level(), state->compaction->InputToString(0).data(),
      state->compaction->level() + 1,
      state->compaction->InputToString(1).data());
  state->compaction->AddInputDeletions(state->compaction->edit());
  const int level = state->compaction->level();
  for (const auto& output : state->outputs) {
    state->compaction->edit()->AddFile(level + 1, output.number,
                                       output.file_size, output.smallest,
                                       output.largest);
  }
  return vset_->LogAndApply(state->compaction->edit(), &mu_);
}

void DBImpl::CompactionMemtable() {
  mu_.AssertHeld();
  VersionEdit edit;
  Status s = WriteLevel0SSTable(imm_, &edit);

  if (s.ok() && closed_.load(std::memory_order_acquire)) {
    s = Status::Corruption("DB is closed during compaction memtable");
  }
  if (s.ok()) {
    // early log is unuseful
    edit.SetLogNumber(logfile_number_);
    s = vset_->LogAndApply(&edit, &mu_);
  }
  if (s.ok()) {
    imm_->Unref();
    imm_ = nullptr;
    has_imm_.store(false, std::memory_order_release);
    GarbageFilesClean();
  } else {
    RecordBackgroundError(s);
  }
}

void DBImpl::RecordBackgroundError(Status s) {
  mu_.AssertHeld();
  if (background_status_.ok()) {
    background_status_ = s;
    background_cv_.SignalAll();
    Log(option_.logger, "RecordBackgroundError: %s\n", s.ToString().data());
  }
}

void DBImpl::GarbageFilesClean() {
  mu_.AssertHeld();
  if (!background_status_.ok()) {
    return;
  }
  std::set<uint64_t> live;
  for (uint64_t number : files_writing_) {
    live.insert(number);
  }
  vset_->AddLiveFiles(&live);

  std::vector<std::string> filenames;
  env_->GetChildren(name_, &filenames);
  uint64_t number;
  FileType type;
  std::vector<std::string> file_delete;

  for (const std::string& filename : filenames) {
    if (ParseFilename(filename, &number, &type)) {
      bool keep = true;
      switch (type) {
        case KLogFile:
          keep = number >= vset_->LogNumber();
          break;
        case KMetaFile:
          keep = number >= vset_->MetaFileNumber();
          break;
        case KSSTableFile:
          keep = live.find(number) != live.end();
          break;
        case KTmpFile:
          keep = false;
          break;
        case KCurrentFile:
        case KLockFile:
        case KLoggerFile:
          keep = true;
          break;
      }
      if (!keep) {
        Log(option_.logger, "Garbage Clean:%s\n", filename.data());
        file_delete.push_back(filename);
        if (type == KSSTableFile) {
          table_cache_->Evict(number);
        }
      }
    }
  }
  // mu_.Unlock();
  for (const std::string& filename : file_delete) {
    env_->RemoveFile(name_ + "/" + filename);
  }
  // mu_.Lock();
}

Status DBImpl::FinishCompactionSSTable(CompactionState* state,
                                       Iterator* input) {
  assert(state->builder != nullptr);
  Status s = input->status();
  uint64_t number = state->CurrOutput()->number;
  uint64_t num_entries = state->builder->NumEntries();
  if (s.ok()) {
    s = state->builder->Finish();
  }
  const uint64_t file_size = state->builder->FileSize();
  std::string file_name = SSTableFileName(name_, number);
  state->CurrOutput()->file_size = file_size;
  state->total_bytes += file_size;
  delete state->builder;
  state->builder = nullptr;
  if (s.ok()) {
    s = state->out_file->Sync();
  }
  if (s.ok()) {
    s = state->out_file->Close();
  }
  uint64_t actual_size;
  env_->FileSize(file_name, &actual_size);
  assert(actual_size == file_size);

  delete state->out_file;
  state->out_file = nullptr;
  if (s.ok() && num_entries > 0) {
    Iterator* iter = table_cache_->NewIterator(ReadOption(), number, file_size);
    s = iter->status();
    delete iter;
    if (s.ok()) {
      Log(option_.logger,
          "Create SSTable #%llu: level:%llu, keys:%llu, bytes:%llu", number,
          state->compaction->level(), num_entries, file_size);
    } else {
      Log(option_.logger, "Create SSTable #%llu: ERROR:%s", number,
          s.ToString().data());
    }
  }
  return s;
}

Status DBImpl::OpenCompactionSSTable(CompactionState* state) {
  // mu_.AssertHeld();
  assert(state != nullptr);
  assert(state->builder == nullptr);
  uint64_t number;
  {
    mu_.Lock();
    number = vset_->NextFileNumber();
    CompactionState::Output output;
    output.number = number;
    files_writing_.insert(number);
    output.smallest.Clear();
    output.largest.Clear();
    state->outputs.push_back(output);
    mu_.Unlock();
  }
  std::string filename = SSTableFileName(name_, number);
  Status s = env_->NewWritableFile(filename, &state->out_file);
  if (s.ok()) {
    state->builder = new SSTableBuilder(option_, state->out_file);
  }
  return s;
}
}  // namespace lsmkv