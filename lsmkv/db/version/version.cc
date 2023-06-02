#include "db/version/version.h"

#include <algorithm>
#include <cassert>

#include "db/log/log_reader.h"
#include "db/version/merge.h"
#include "db/version/version_edit.h"
#include "include/env.h"
#include "util/filename.h"

namespace lsmkv {
VersionSet::VersionSet(const std::string name, const Option* option,
                       TableCache* cache, const InternalKeyComparator* cmp)
    : name_(name),
      option_(option),
      env_(option->env),
      icmp_(*cmp),
      table_cache_(cache),
      dummy_head_(this),
      current_(nullptr),
      log_number_(0),
      last_sequence_(0),
      next_file_number_(2),
      meta_file_number_(0),
      meta_log_file_(nullptr),
      meta_log_writer_(nullptr) {
  AppendVersion(new Version(this));
}
VersionSet::~VersionSet() {
  current_->Unref();
  assert(dummy_head_.next_ == &dummy_head_);
  delete meta_log_file_;
  delete meta_log_writer_;
}
Version::~Version() {
  assert(refs_ == 0);
  next_->prev_ = prev_;
  prev_->next_ = next_;

  for (int level = 0; level < config::kNumLevels; level++) {
    for (FileMeta* meta : files_[level]) {
      assert(meta->refs > 0);
      meta->refs--;
      if (meta->refs == 0) {
        delete meta;
      }
    }
  }
}

enum SaverState {
  KNotFound,
  KFound,
  KDeleted,
  KCorrupt,
};

struct GetSaver {
  std::string_view user_key;
  std::string* result;
  const Comparator* user_cmp;
  SaverState state;
};

static void SaveResult(void* arg, std::string_view key,
                       std::string_view value) {
  GetSaver* saver = reinterpret_cast<GetSaver*>(arg);
  ParsedInternalKey parsed_key;
  if (!ParseInternalKey(key, &parsed_key)) {
    saver->state = KCorrupt;
  } else {
    if (saver->user_cmp->Compare(parsed_key.user_key_, saver->user_key) == 0) {
      saver->state = (parsed_key.type_ == KTypeInsertion ? KFound : KDeleted);
      if (saver->state == KFound) {
        saver->result->assign(value.data(), value.size());
      }
    }
  }
}

static bool NewFirst(FileMeta* a, FileMeta* b) { return a->number > b->number; }

// find first file that "largest >= internal_key"
size_t FindFile(const std::vector<FileMeta*>& files, std::string_view user_key,
                const Comparator* ucmp) {
  size_t lo = 0;
  size_t hi = files.size();
  while (lo < hi) {
    // if (lo == hi - 1), mid will be lo.
    // to avoid dead loop
    size_t mid = (lo + hi) / 2;
    FileMeta* meta = files[mid];
    if (ucmp->Compare(user_key, meta->largest.user_key()) > 0) {
      // all file before or at mid is "largest < internal_key"
      lo = mid + 1;
    } else {
      // mid is "largest >= internal_key",
      // all file after it is unuseful
      hi = mid;
    }
  }
  return hi;
}

void Version::ForEachFile(std::string_view user_key,
                          std::string_view internal_key, void* arg,
                          bool (*fun)(void*, int, FileMeta*)) {
  const Comparator* ucmp = vset_->icmp_.UserComparator();
  std::vector<FileMeta*> tmp;
  // 如果user_key处于meta的SST文件中， 将meta存进tmp
  for (FileMeta* meta : files_[0]) {
    if (ucmp->Compare(user_key, meta->largest.user_key()) <= 0 &&
        ucmp->Compare(user_key, meta->smallest.user_key()) >= 0) {
      tmp.push_back(meta);
    }
  }
  if (!tmp.empty()) {
    std::sort(tmp.begin(), tmp.end(), NewFirst);
    for (FileMeta* meta : tmp) {
      if (!(*fun)(arg, 0, meta)) {
        return;
      }
    }
  }
  for (int level = 1; level < config::kNumLevels; level++) {
    if (files_[level].size() == 0) {
      continue;
    }
    size_t index = FindFile(files_[level], user_key, ucmp);
    if (index < files_[level].size()) {
      FileMeta* meta = files_[level][index];
      if (ucmp->Compare(user_key, meta->smallest.user_key()) >= 0) {
        if (!(*fun)(arg, level, meta)) {
          return;
        }
      }
    }
  }
}
Status Version::Get(const ReadOption& option, const LookupKey& key,
                    std::string* result, GetStats* stats) {
  stats->seek_file = nullptr;
  stats->seek_file_level = -1;

  struct State {
    GetSaver saver;
    GetStats* stats;
    const ReadOption* option;
    std::string_view internal_key;
    Status s;
    VersionSet* vset;
    bool found;

    int last_seek_file_level;
    FileMeta* last_seek_file;

    /**
     * @brief
     * @details 通过ForEachFile(state.saver.user_key, state.internal_key,
     &state, &State::SSTableMatch);调用
     * @param[in] arg
     * @param[in] level
     * @param[in] meta
     * @return true
     * @return false
     */
    static bool SSTableMatch(void* arg, int level, FileMeta* meta) {
      State* state = reinterpret_cast<State*>(arg);
      if (state->last_seek_file != nullptr &&
          state->stats->seek_file == nullptr) {
        state->stats->seek_file = state->last_seek_file;
        state->stats->seek_file_level = state->last_seek_file_level;
      }
      state->last_seek_file = meta;
      state->last_seek_file_level = level;

      state->s = state->vset->table_cache_->Get(
          *state->option, meta->number, meta->file_size, state->internal_key,
          &state->saver, &SaveResult);
      if (!state->s.ok()) {
        state->found = true;
        return false;
      }
      switch (state->saver.state) {
        case KCorrupt:
          state->found = true;
          state->s = Status::Corruption("incorrect parse key for ",
                                        state->saver.user_key);
          return false;
        case KFound:
          state->found = true;
          return false;
        case KDeleted:
          return false;
        case KNotFound:
          return true;  // keep searching
      }
      return false;
    }
  };

  State state;
  state.saver.user_key = key.UserKey();
  state.saver.state = KNotFound;
  state.saver.result = result;
  state.saver.user_cmp = vset_->icmp_.UserComparator();
  state.last_seek_file = nullptr;
  state.last_seek_file_level = -1;

  state.option = &option;
  state.internal_key = key.InternalKey();
  state.vset = vset_;
  state.found = false;

  ForEachFile(state.saver.user_key, state.internal_key, &state,
              &State::SSTableMatch);

  return state.found ? state.s
                     : Status::NotFound("key is not found in sstable");
}

void Version::Ref() { ++refs_; }

void Version::Unref() {
  assert(refs_ >= 1);
  --refs_;
  if (refs_ == 0) {
    delete this;
  }
}

class VersionSet::Builder {
 public:
  Builder(VersionSet* vset, Version* base) : vset_(vset), base_(base) {
    base->Ref();
    BySmallestKey cmp;
    cmp.icmp_ = &vset->icmp_;
    for (int i = 0; i < config::kNumLevels; i++) {
      level_[i].add_files_ = new AddSet(cmp);
    }
  }
  ~Builder() {
    for (int i = 0; i < config::kNumLevels; i++) {
      AddSet* add_files = level_[i].add_files_;
      std::vector<FileMeta*> to_unref;
      to_unref.reserve(add_files->size());
      for (AddSet::iterator it = add_files->begin(); it != add_files->end();
           ++it) {
        to_unref.push_back(*it);
      }
      delete add_files;
      for (FileMeta* meta : to_unref) {
        meta->refs--;
        if (meta->refs <= 0) {
          delete meta;
        }
      }
    }
    base_->Unref();
  }
  void Apply(const VersionEdit* edit) {
    for (const auto& compaction_pointer : edit->compaction_pointers_) {
      const int level = compaction_pointer.first;
      vset_->compactor_pointer_[level] = compaction_pointer.second.Encode();
    }
    for (const auto& file : edit->delete_files_) {
      const int level = file.first;
      const uint64_t file_number = file.second;
      level_[level].deleted_files.insert(file_number);
    }
    for (const auto& file : edit->new_files_) {
      const int level = file.first;
      FileMeta* meta = new FileMeta(file.second);
      meta->refs = 1;

      // assume a seek costs equal to 40KB data
      // So, allow one seek for each 16KB data.
      meta->allow_seeks = static_cast<int>(meta->file_size / 16384U);
      if (meta->allow_seeks < 100) meta->allow_seeks = 100;

      level_[level].deleted_files.erase(meta->number);
      level_[level].add_files_->insert(meta);
    }
  }
  void SaveTo(Version* v) {
    BySmallestKey cmp;
    cmp.icmp_ = &vset_->icmp_;
    for (int i = 0; i < config::kNumLevels; i++) {
      const std::vector<FileMeta*>& base_files = base_->files_[i];
      std::vector<FileMeta*>::const_iterator base_iter = base_files.begin();
      std::vector<FileMeta*>::const_iterator base_end = base_files.end();
      const AddSet* add_files = level_[i].add_files_;
      v->files_[i].reserve(base_files.size() + add_files->size());
      for (const auto& add_file : *add_files) {
        for (std::vector<FileMeta*>::const_iterator base_pos =
                 std::upper_bound(base_iter, base_end, add_file, cmp);
             base_iter != base_pos; ++base_iter) {
          AddFile(v, i, *base_iter);
        }
        AddFile(v, i, add_file);
      }
      for (; base_iter != base_end; ++base_iter) {
        AddFile(v, i, *base_iter);
      }
    }
  }
  void AddFile(Version* v, int level, FileMeta* meta) {
    if (level_[level].deleted_files.count(meta->number)) {
      return;
    }
    std::vector<FileMeta*>* files = &v->files_[level];
    if (level > 0 && !files->empty()) {
      if (vset_->icmp_.Compare((*files)[files->size() - 1]->largest,
                               meta->smallest) >= 0) {
        assert(vset_->icmp_.Compare((*files)[files->size() - 1]->largest,
                                    meta->smallest) < 0);
      }
    }
    meta->refs++;
    files->push_back(meta);
  }

 private:
  struct BySmallestKey {
    const InternalKeyComparator* icmp_;

    bool operator()(FileMeta* f1, FileMeta* f2) const {
      int r = icmp_->Compare(f1->smallest, f2->smallest);
      if (r != 0) {
        return (r < 0);
      } else {
        return (f1->number < f2->number);
      }
    }
  };
  using AddSet = std::set<FileMeta*, BySmallestKey>;
  struct LevelState {
    std::set<uint64_t> deleted_files;
    AddSet* add_files_;
  };

  Version* base_;
  VersionSet* vset_;
  LevelState level_[config::kNumLevels];
};

Status VersionSet::Recover() {
  std::string current;
  Status s = ReadStringFromFile(env_, &current, CurrentFileName(name_));
  if (!s.ok()) {
    return s;
  }
  SequentialFile* file;
  s = env_->NewSequentialFile(current, &file);
  if (!s.ok()) {
    if (s.IsNotFound()) {
      return Status::Corruption(
          "CURRENT file points to a not-existing Meta file");
    }
    return s;
  }
  uint64_t log_number = 0;
  SequenceNum last_sequence = 0;
  uint64_t next_file_number = 0;
  std::string comparator_name;
  bool has_log_number = false;
  bool has_last_sequence = false;
  bool has_next_file_number = false;
  bool has_comparator_name = false;
  Builder builder(this, current_);

  {
    log::Reader reader(file, true, 0);
    std::string_view sv;
    std::string buffer;
    while (reader.ReadRecord(&sv, &buffer) && s.ok()) {
      VersionEdit edit;
      s = edit.DecodeFrom(sv);
      if (s.ok()) {
        if (edit.has_comparator_name_ &&
            edit.comparator_name_ != icmp_.UserComparator()->Name()) {
          s = Status::Corruption(edit.comparator_name_ + "don't match " +
                                 icmp_.UserComparator()->Name());
        }
      }
      if (s.ok()) {
        builder.Apply(&edit);
      }
      if (edit.has_log_number_) {
        has_log_number = true;
        log_number = edit.log_number_;
      }
      if (edit.has_last_sequence_) {
        has_last_sequence = true;
        last_sequence = edit.last_sequence_;
      }
      if (edit.has_next_file_number_) {
        has_next_file_number = true;
        next_file_number = edit.next_file_number_;
      }
    }
  }
  delete file;

  if (s.ok()) {
    if (!has_log_number) {
      s = Status::Corruption("no log_number in meta file");
    }
    if (!has_last_sequence) {
      s = Status::Corruption("no last_sequence in meta file");
    }
    if (!has_next_file_number) {
      s = Status::Corruption("no next_file_number in meta file");
    }
    MarkFileNumberUsed(log_number);
  }

  if (s.ok()) {
    Version* v = new Version(this);
    builder.SaveTo(v);
    AppendVersion(v);
    EvalCompactionScore(v);
    log_number_ = log_number;
    meta_file_number_ = next_file_number;
    last_sequence_ = last_sequence;
    next_file_number_ = next_file_number + 1;
  }
  return s;
}

Status VersionSet::LogAndApply(VersionEdit* edit, Mutex* mu) {
  if (edit->has_log_number_) {
    assert(edit->log_number_ >= log_number_);
    assert(edit->log_number_ < next_file_number_);
  } else {
    edit->SetLogNumber(log_number_);
  }
  edit->SetLastSequence(last_sequence_);
  edit->SetNextFileNumber(next_file_number_);

  Version* v = new Version(this);

  Builder builder(this, current_);
  builder.Apply(edit);
  builder.SaveTo(v);

  Status s;
  std::string meta_file_name;

  bool initialize = false;
  if (meta_log_writer_ == nullptr) {
    assert(meta_log_file_ == nullptr);
    initialize = true;
    meta_file_name = MetaFileName(name_, meta_file_number_);
    s = env_->NewWritableFile(meta_file_name, &meta_log_file_);
    if (s.ok()) {
      meta_log_writer_ = new log::Writer(meta_log_file_);
      s = WriteSnapShot(meta_log_writer_);
    }
  }

  {
    // NOTE: Only one Compaction is running at one time
    // so unlock is safe here;
    mu->Unlock();
    if (s.ok()) {
      std::string record;
      edit->EncodeTo(&record);
      s = meta_log_writer_->AddRecord(record);
      if (s.ok()) {
        s = meta_log_file_->Sync();
      }
    }
    if (s.ok() && initialize) {
      s = SetCurrentFile(env_, name_, meta_file_number_);
    }
    mu->Lock();
  }

  if (s.ok()) {
    AppendVersion(v);
    EvalCompactionScore(v);
    log_number_ = edit->log_number_;
  } else {
    delete v;
    if (initialize) {
      delete meta_log_file_;
      delete meta_log_writer_;
      meta_log_file_ = nullptr;
      meta_log_writer_ = nullptr;
      env_->RemoveFile(meta_file_name);
    }
  }
  return s;
}

Status VersionSet::WriteSnapShot(log::Writer* writer) {
  VersionEdit edit;

  edit.SetComparatorName(icmp_.UserComparator()->Name());

  for (int level = 0; level < config::kNumLevels; level++) {
    if (!compactor_pointer_[level].empty()) {
      InternalKey key;
      key.DecodeFrom(compactor_pointer_[level]);
      edit.SetCompactionPointer(level, key);
    }
  }

  for (int level = 0; level < config::kNumLevels; level++) {
    const std::vector<FileMeta*>& files = current_->files_[level];
    for (const FileMeta* meta : files) {
      edit.AddFile(level, meta->number, meta->file_size, meta->smallest,
                   meta->largest);
    }
  }

  std::string record;
  edit.EncodeTo(&record);
  return writer->AddRecord(record);
}

void VersionSet::AppendVersion(Version* v) {
  assert(v->refs_ == 0);
  assert(current_ != v);

  if (current_ != nullptr) {
    current_->Unref();
  }
  v->Ref();
  current_ = v;

  v->next_ = &dummy_head_;
  v->prev_ = dummy_head_.prev_;
  v->next_->prev_ = v;
  v->prev_->next_ = v;
}

void VersionSet::AddLiveFiles(std::set<uint64_t>* live) {
  for (Version* v = dummy_head_.next_; v != &dummy_head_; v = v->next_) {
    for (int level = 0; level < config::kNumLevels; level++) {
      const std::vector<FileMeta*>& files_ = v->files_[level];
      for (const FileMeta* file : files_) {
        live->insert(file->number);
      }
    }
  }
}

static uint64_t TotalFileSize(std::vector<FileMeta*> files) {
  uint64_t sum = 0;
  for (FileMeta* meta : files) {
    sum += meta->file_size;
  }
  return sum;
}

static double LevelMaxSize(int level) {
  // the constants is from Leveldb.
  // may be a experience value.
  double result = 10. * 1048576.0;
  while (level > 1) {
    result *= 10;
    level--;
  }
  return result;
}

static uint64_t ExpandCompactionLimit(const Option* option) {
  return 25 * option->max_file_size;
}

static uint64_t GrandparantsOverLapLimit(const Option* option) {
  return 10 * option->max_file_size;
}

static uint64_t SSTableFileLimit(const Option* option) {
  return option->max_file_size;
}

void VersionSet::EvalCompactionScore(Version* v) {
  int best_level = -1;
  double best_score = -1;

  for (int level = 0; level < config::kNumLevels; ++level) {
    double score;
    if (level == 0) {
      score = v->files_[level].size() / config::kL0CompactionThreshold;
    } else {
      const uint64_t file_size = TotalFileSize(v->files_[level]);
      score = static_cast<double>(file_size) / LevelMaxSize(level);
    }
    if (score > best_score) {
      best_level = level;
      best_score = score;
    }
  }
  v->compaction_level = best_level;
  v->compaction_score = best_score;
}

bool Version::UpdateStats(const GetStats& stats) {
  FileMeta* meta = stats.seek_file;
  if (meta != nullptr) {
    meta->allow_seeks--;
    if (meta->allow_seeks <= 0 && file_to_compact_ == nullptr) {
      file_to_compact_ = meta;
      file_to_compact_level_ = stats.seek_file_level;
      return true;
    }
  }
  return false;
}

void VersionSet::GetRange(const std::vector<FileMeta*>& input,
                          InternalKey* smallest, InternalKey* largest) {
  assert(!input.empty());
  smallest->Clear();
  largest->Clear();
  bool first = true;
  for (const FileMeta* meta : input) {
    if (first) {
      *smallest = meta->smallest;
      *largest = meta->largest;
    } else {
      if (icmp_.Compare(*smallest, meta->smallest) < 0) {
        *smallest = meta->smallest;
      }
      if (icmp_.Compare(*largest, meta->largest) > 0) {
        *largest = meta->largest;
      }
    }
    first = false;
  }
}

void VersionSet::GetTwoRange(const std::vector<FileMeta*>& input1,
                             const std::vector<FileMeta*>& input2,
                             InternalKey* smallest, InternalKey* largest) {
  std::vector<FileMeta*> input_all = input1;
  input_all.insert(input_all.end(), input2.begin(), input2.end());
  GetRange(input_all, smallest, largest);
}

void Version::GetOverlappingFiles(int level, const InternalKey& smallest,
                                  const InternalKey& largest,
                                  std::vector<FileMeta*>* input) {
  input->clear();
  std::string_view user_smallest = smallest.user_key();
  std::string_view user_largest = largest.user_key();

  const Comparator* ucmp = vset_->icmp_.UserComparator();
  for (int i = 0; i < files_[level].size();) {
    FileMeta* meta = files_[level][i++];
    const std::string_view file_smallest = meta->smallest.user_key();
    const std::string_view file_largest = meta->largest.user_key();
    if (ucmp->Compare(file_smallest, user_largest) > 0) {
      // file is after range
    } else if (ucmp->Compare(file_largest, user_smallest) < 0) {
      // file is before range
    } else {
      input->push_back(meta);
      // TODO:如果是Level 0层，？？？
      if (level == 0) {
        if (ucmp->Compare(file_smallest, user_smallest) < 0) {
          i = 0;
          input->clear();
          user_smallest = file_smallest;
        } else if (ucmp->Compare(file_largest, user_largest) > 0) {
          i = 0;
          input->clear();
          user_largest = file_largest;
        }
      }
    }
  }
}
FileMeta* FindSmallestBoundary(const InternalKeyComparator* icmp,
                               const std::vector<FileMeta*>& level_files,
                               const InternalKey& largest_key) {
  const Comparator* ucmp = icmp->UserComparator();
  FileMeta* smallest_boundary = nullptr;
  for (FileMeta* meta : level_files) {
    if (icmp->Compare(largest_key, meta->largest) < 0 &&
        ucmp->Compare(largest_key.user_key(), meta->largest.user_key()) == 0 &&
        (smallest_boundary == nullptr ||
         icmp->Compare(meta->largest, smallest_boundary->largest) < 0)) {
      smallest_boundary = meta;
    }
  }
  return smallest_boundary;
}

void AddBoundaryInputs(const InternalKeyComparator* icmp,
                       const std::vector<FileMeta*>& level_files,
                       std::vector<FileMeta*>* inputs) {
  InternalKey largest_key;
  if (level_files.empty()) {
    return;
  }
  largest_key = level_files[0]->largest;
  for (size_t i = 1; i < level_files.size(); i++) {
    if (icmp->Compare(largest_key, level_files[i]->largest) < 0) {
      largest_key = level_files[i]->largest;
    }
  }
  while (true) {
    FileMeta* meta = FindSmallestBoundary(icmp, level_files, largest_key);
    if (meta == nullptr) {
      break;
    }
    inputs->push_back(meta);
    largest_key = meta->largest;
  }
}
Compaction* VersionSet::PickCompaction() {
  Compaction* c;
  int level;

  bool size_compaction = (current_->compaction_score >= 1);
  bool seek_compaction = (current_->file_to_compact_ != nullptr);
  if (size_compaction) {
    level = current_->compaction_level;
    assert(level >= 0);
    assert(level <= config::kL0CompactionThreshold);
    c = new Compaction(option_, level);
    for (FileMeta* meta : current_->files_[level]) {
      if (compactor_pointer_[level].empty() ||
          icmp_.Compare(meta->largest.Encode(), compactor_pointer_[level]) >
              0) {
        c->input_[0].push_back(meta);
        break;
      }
    }
    if (c->input_[0].empty()) {
      c->input_[0].push_back(current_->files_[level][0]);
    }
  } else if (seek_compaction) {
    level = current_->file_to_compact_level_;
    c = new Compaction(option_, level);
    c->input_[0].push_back(current_->file_to_compact_);
  } else {
    return nullptr;
  }

  c->input_version_ = current_;
  c->input_version_->Ref();

  // files in level-0 is possible over range.
  // collect all file ovp er range with the original file
  if (level == 0) {
    InternalKey smallest, largest;
    GetRange(c->input_[0], &smallest, &largest);
    current_->GetOverlappingFiles(0, smallest, largest, &c->input_[0]);
  }

  InternalKey smallest, largest;
  AddBoundaryInputs(&icmp_, current_->files_[level], &c->input_[0]);
  GetRange(c->input_[0], &smallest, &largest);
  current_->GetOverlappingFiles(level + 1, smallest, largest, &c->input_[1]);
  AddBoundaryInputs(&icmp_, current_->files_[level + 1], &c->input_[1]);

  InternalKey all_smallest, all_largest;
  GetTwoRange(c->input_[0], c->input_[1], &all_smallest, &all_largest);

  // try to grow the number of inputs[0], without change
  // inputs[1]
  if (!c->input_[1].empty()) {
    std::vector<FileMeta*> expand0;
    current_->GetOverlappingFiles(level, all_smallest, all_largest, &expand0);
    AddBoundaryInputs(&icmp_, current_->files_[level], &expand0);
    const uint64_t expand0_size = TotalFileSize(expand0);
    const uint64_t input0_size = TotalFileSize(c->input_[0]);
    const uint64_t input1_size = TotalFileSize(c->input_[1]);
    if (expand0_size > input0_size &&
        expand0_size < ExpandCompactionLimit(option_)) {
      InternalKey new_smallest, new_largest;
      std::vector<FileMeta*> expand1;
      GetRange(expand0, &new_smallest, &new_largest);
      current_->GetOverlappingFiles(level + 1, new_smallest, new_largest,
                                    &expand1);
      AddBoundaryInputs(&icmp_, current_->files_[level + 1], &expand1);
      if (expand1.size() == c->input_[1].size()) {
        smallest = new_smallest;
        largest = new_largest;
        c->input_[0] = expand0;
        c->input_[1] = expand1;
        GetTwoRange(c->input_[0], c->input_[1], &all_smallest, &all_largest);
      }
    }
  }
  if (level + 2 < config::kNumLevels) {
    current_->GetOverlappingFiles(level + 2, all_smallest, all_largest,
                                  &c->grandparents_);
  }
  c->edit_.SetCompactionPointer(level, largest);
  return c;
}

bool Compaction::SingalMove() const {
  return (input_[0].size() == 1 && input_[1].size() == 0 &&
          TotalFileSize(grandparents_) <
              GrandparantsOverLapLimit(input_version_->vset_->option_));
}

void Compaction::AddInputDeletions(VersionEdit* edit) {
  for (int which = 0; which < 2; which++) {
    for (FileMeta* meta : input_[which]) {
      edit->DeleteFile(level_ + which, meta->number);
    }
  }
}

class Version::LevelFileIterator : public Iterator {
 public:
  LevelFileIterator(const InternalKeyComparator icmp,
                    const std::vector<FileMeta*>* input)
      : icmp_(icmp), input_(input), index_(input->size()) {}

  ~LevelFileIterator() = default;

  bool Valid() const override { return index_ < input_->size(); }

  std::string_view Key() const override {
    assert(Valid());
    return (*input_)[index_]->largest.Encode();
  }

  std::string_view Value() const override {
    assert(Valid());
    EncodeFixed64(buf_, (*input_)[index_]->file_size);
    EncodeFixed64(buf_ + 8, (*input_)[index_]->number);
    return std::string_view(buf_, 16);
  }

  void Next() override {
    assert(Valid());
    index_++;
  }

  void Prev() override {
    assert(Valid());
    index_ = index_ == 0 ? input_->size() : index_ - 1;
  }

  void Seek(std::string_view key) override {
    index_ = FindFile(*input_, key, icmp_.UserComparator());
  }

  void SeekToFirst() override { index_ = 0; }

  void SeekToLast() override {
    index_ = input_->size() == 0 ? 0 : input_->size() - 1;
  }

  Status status() override { return Status::OK(); }

 private:
  const InternalKeyComparator icmp_;
  const std::vector<FileMeta*>* input_;
  size_t index_;
  mutable char buf_[16];
};

Iterator* GetFileIterator(void* arg, const ReadOption& option,
                          std::string_view handle_contents) {
  TableCache* cache = reinterpret_cast<TableCache*>(arg);
  if (handle_contents.size() != 16) {
    return NewErrorIterator(
        Status::Corruption("LevelFileIterator value is wrong"));
  }
  uint64_t file_size = DecodeFixed64(handle_contents.data());
  uint64_t number = DecodeFixed64(handle_contents.data() + 8);
  return cache->NewIterator(option, number, file_size);
}
Iterator* VersionSet::MakeMergedIterator(Compaction* c) {
  ReadOption option;
  option.check_crc = option_->check_crc;
  const size_t space = (c->level() == 0 ? 1 + c->input_[0].size() : 2);
  Iterator** list = new Iterator*[space];
  size_t idx = 0;
  for (int which = 0; which < 2; which++) {
    if (!c->input_[which].empty()) {
      if (which + c->level_ == 0) {
        const std::vector<FileMeta*>& input = c->input_[which];
        for (const FileMeta* meta : input) {
          list[idx++] =
              table_cache_->NewIterator(option, meta->number, meta->file_size);
        }
      } else {
        list[idx++] = NewTwoLevelIterator(
            new Version::LevelFileIterator(icmp_, &c->input_[which]),
            &GetFileIterator, table_cache_, option);
      }
    }
  }

  assert(idx <= space);
  Iterator* ret = NewMergedIterator(list, idx, &icmp_);
  delete[] list;
  return ret;
}
Compaction::Compaction(const Option* option, int level)
    : level_(level),
      max_output_file_bytes_(SSTableFileLimit(option)),
      input_version_(nullptr),
      grandparents_overlap_(0),
      grandparents_index_(0),
      seen_key_(false) {}

bool Compaction::StopBefore(std::string_view key) {
  const InternalKeyComparator* icmp = &input_version_->vset_->icmp_;
  while (grandparents_index_ < grandparents_.size() &&
         icmp->Compare(
             key, grandparents_[grandparents_index_]->largest.Encode()) > 0) {
    if (seen_key_) {
      grandparents_overlap_ += grandparents_[grandparents_index_]->file_size;
    }
    ++grandparents_index_;
  }
  // to seek the first input key in the grandparents
  seen_key_ = true;
  if (grandparents_overlap_ >
      GrandparantsOverLapLimit(input_version_->vset_->option_)) {
    grandparents_overlap_ = 0;
    return true;
  }
  return false;
}

bool Compaction::IsBaseLevelForKey(std::string_view key) {
  const Comparator* ucmp = input_version_->vset_->icmp_.UserComparator();
  for (int level = level_ + 2; level < config::kNumLevels; level++) {
    for (auto meta : input_version_->files_[level]) {
      if (ucmp->Compare(key, meta->smallest.user_key()) >= 0 &&
          ucmp->Compare(key, meta->largest.user_key()) <= 0) {
        return false;
      }
    }
  }
  return true;
}
}  // namespace lsmkv