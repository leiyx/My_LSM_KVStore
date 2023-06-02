#include "include/iterator.h"

#include <cassert>

#include "include/option.h"

namespace lsmkv {
Iterator::Iterator() {
  cleanup_head_.fun = nullptr;
  cleanup_head_.next = nullptr;
}

void Iterator::AppendCleanup(CleanupFunction fun, void* arg1, void* arg2) {
  assert(fun != nullptr);
  CleanupNode* node;
  if (cleanup_head_.fun == nullptr) {
    node = &cleanup_head_;
  } else {
    node = new CleanupNode();
    node->next = cleanup_head_.next;
    cleanup_head_.next = node;
  }
  node->fun = fun;
  node->arg1 = arg1;
  node->arg2 = arg2;
}

Iterator::~Iterator() {
  if (cleanup_head_.fun != nullptr) {
    cleanup_head_.Run();
    CleanupNode* node = cleanup_head_.next;
    while (node != nullptr) {
      node->Run();
      CleanupNode* next = node->next;
      delete node;
      node = next;
    }
  }
}
class EmptyIterator : public Iterator {
 public:
  EmptyIterator(const Status s) : status_(s) {}

  EmptyIterator(const EmptyIterator&) = delete;
  EmptyIterator& operator=(const EmptyIterator&) = delete;

  ~EmptyIterator() = default;

  bool Valid() const { return false; }
  void Next() { assert(false); }
  void Prev() { assert(false); }
  void Seek(std::string_view key) {}
  void SeekToFirst() {}
  void SeekToLast() {}
  Status status() { return status_; }
  std::string_view Key() const {
    assert(false);
    return std::string_view();
  }
  std::string_view Value() const {
    assert(false);
    return std::string_view();
  }

 private:
  Status status_;
};

Iterator* NewEmptyIterator() { return new EmptyIterator(Status::OK()); }

Iterator* NewErrorIterator(Status status) { return new EmptyIterator(status); }

using BlockFunction = Iterator* (*)(void* arg, const ReadOption& option,
                                    std::string_view handle_contents);

class TwoLevelIterator : public Iterator {
 public:
  TwoLevelIterator(Iterator* index_iter, BlockFunction block_funtion, void* arg,
                   const ReadOption& option);
  ~TwoLevelIterator() override {
    delete data_iter_;
    delete index_iter_;
  }
  bool Valid() const override {
    return data_iter_ != nullptr && data_iter_->Valid();
  }

  std::string_view Key() const override {
    assert(Valid());
    return data_iter_->Key();
  }

  std::string_view Value() const override {
    assert(Valid());
    return data_iter_->Value();
  }
  void Next() override;
  void Prev() override;
  void Seek(std::string_view key) override;
  void SeekToFirst() override;
  void SeekToLast() override;

  Status status() override {
    if (!index_iter_->status().ok()) {
      return index_iter_->status();
    } else if (data_iter_ != nullptr && !data_iter_->status().ok()) {
      return data_iter_->status();
    }
    return status_;
  }

 private:
  void UpdateDataBlock();
  void SetDataIterator(Iterator* data_iter);
  void SaveError(const Status& s) {
    if (status_.ok() && !s.ok()) {
      status_ = s;
    }
  }

  void SkipEmptyDataBlock(bool is_forward);

  BlockFunction block_function_;
  Iterator* index_iter_;
  Iterator* data_iter_;
  void* args_;
  const ReadOption option_;
  Status status_;
  std::string data_block_handle_;
};
Iterator* NewTwoLevelIterator(
    Iterator* index_iter,
    Iterator* (*block_funtion)(void* arg, const ReadOption& option,
                               std::string_view handle_contents),
    void* arg, const ReadOption& option) {
  return new TwoLevelIterator(index_iter, block_funtion, arg, option);
}

TwoLevelIterator::TwoLevelIterator(Iterator* index_iter,
                                   BlockFunction block_funtion, void* arg,
                                   const ReadOption& option)
    : index_iter_(index_iter),
      data_iter_(nullptr),
      block_function_(block_funtion),
      args_(arg),
      option_(option) {}

void TwoLevelIterator::SetDataIterator(Iterator* data_iter) {
  if (data_iter_ != nullptr) {
    SaveError(data_iter_->status());
  }
  delete data_iter_;
  data_iter_ = data_iter;
}
void TwoLevelIterator::UpdateDataBlock() {
  if (!index_iter_->Valid()) {
    SetDataIterator(nullptr);
  } else {
    std::string_view handle = index_iter_->Value();
    if (data_iter_ != nullptr && handle.compare(data_block_handle_) == 0) {
      return;
    }
    Iterator* iter = (*block_function_)(args_, option_, handle);
    data_block_handle_.assign(handle.data(), handle.size());
    SetDataIterator(iter);
  }
}

void TwoLevelIterator::SkipEmptyDataBlock(bool is_forward) {
  while (data_iter_ == nullptr || !data_iter_->Valid()) {
    if (!index_iter_->Valid()) {
      SetDataIterator(nullptr);
      return;
    }
    if (is_forward) {
      index_iter_->Next();
      UpdateDataBlock();
      if (data_iter_ != nullptr) {
        data_iter_->SeekToFirst();
      }
    } else {
      index_iter_->Prev();
      UpdateDataBlock();
      if (data_iter_ != nullptr) {
        data_iter_->SeekToLast();
      }
    }
  }
}

void TwoLevelIterator::Next() {
  assert(Valid());
  data_iter_->Next();
  SkipEmptyDataBlock(true);
}

void TwoLevelIterator::Prev() {
  assert(Valid());
  data_iter_->Prev();
  SkipEmptyDataBlock(false);
}

void TwoLevelIterator::Seek(std::string_view key) {
  index_iter_->Seek(key);
  UpdateDataBlock();
  if (data_iter_ != nullptr) {
    data_iter_->Seek(key);
  }
  SkipEmptyDataBlock(true);
}

void TwoLevelIterator::SeekToFirst() {
  index_iter_->SeekToFirst();
  UpdateDataBlock();
  if (data_iter_ != nullptr) {
    data_iter_->SeekToFirst();
  }
  SkipEmptyDataBlock(true);
}

void TwoLevelIterator::SeekToLast() {
  index_iter_->SeekToLast();
  UpdateDataBlock();
  if (data_iter_ != nullptr) {
    data_iter_->SeekToLast();
  }
  SkipEmptyDataBlock(false);
}
}  // namespace lsmkv