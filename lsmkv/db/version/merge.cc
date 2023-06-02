
#include "db/version/merge.h"

#include "db/version/iterator_wrapper.h"

namespace lsmkv {

class MergedIterator : public Iterator {
 public:
  MergedIterator(Iterator** list, size_t num, const Comparator* cmp)
      : list_(new IteratorWrapper[num]),
        num_(num),
        current_(nullptr),
        cmp_(cmp) {
    for (int i = 0; i < num; i++) {
      list_[i].Set(list[i]);
    }
  }

  ~MergedIterator() override { delete[] list_; }
  bool Valid() const override { return current_ != nullptr; }

  std::string_view Key() const override {
    assert(Valid());
    return current_->Key();
  }

  std::string_view Value() const override {
    assert(Valid());
    return current_->Value();
  }

  void Next() override {
    assert(Valid());
    current_->Next();
    FindSmallest();
  }

  /// 不会调用该函数
  void Prev() override { assert(false); }

  void Seek(std::string_view key) override {
    for (int i = 0; i < num_; i++) {
      list_[i].Seek(key);
    }
    FindSmallest();
  }

  void SeekToFirst() override {
    for (int i = 0; i < num_; i++) {
      list_[i].SeekToFirst();
    }
    FindSmallest();
  }
  void SeekToLast() override { assert(false); }

  Status status() override {
    Status status;
    for (int i = 0; i < num_; i++) {
      status = list_[i].status();
      if (!status.ok()) {
        return status;
      }
    }
    return Status::OK();
  }

 private:
  void FindSmallest();

 private:
  IteratorWrapper* list_;
  size_t num_;
  IteratorWrapper* current_;
  const Comparator* cmp_;
};

void MergedIterator::FindSmallest() {
  IteratorWrapper* smallest = nullptr;
  for (size_t i = 0; i < num_; i++) {
    IteratorWrapper* ptr = &list_[i];
    if (ptr->Valid()) {
      if (smallest == nullptr ||
          cmp_->Compare(ptr->Key(), smallest->Key()) < 0) {
        smallest = ptr;
      }
    }
  }
  current_ = smallest;
}

Iterator* NewMergedIterator(Iterator** list, size_t num,
                            const Comparator* cmp) {
  return new MergedIterator(list, num, cmp);
}

}  // namespace lsmkv