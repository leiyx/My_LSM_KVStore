#include "include/sstable_builder.h"

#include <cassert>
#include <iostream>

#include "crc32c/crc32c.h"
#include "db/filter/filter_block.h"
#include "db/sstable/block_builder.h"
#include "db/sstable/block_format.h"
#include "db/version/version_edit.h"
#include "include/comparator.h"
#include "include/iterator.h"
#include "include/option.h"
#include "snappy.h"
#include "util/coding.h"
#include "util/filename.h"

namespace lsmkv {

Status BuildSSTable(const std::string name, const Option& option,
                    TableCache* table_cache, Iterator* iter, FileMeta* meta) {
  Status s;
  meta->file_size = 0;
  iter->SeekToFirst();
  std::string filename = SSTableFileName(name, meta->number);

  if (iter->Valid()) {
    WritableFile* file;
    s = option.env->NewWritableFile(filename, &file);
    if (!s.ok()) {
      return s;
    }
    SSTableBuilder* builder = new SSTableBuilder(option, file);
    meta->smallest.DecodeFrom(iter->Key());
    std::string_view key;
    for (; iter->Valid(); iter->Next()) {
      key = iter->Key();
      builder->Add(key, iter->Value());
    }
    meta->largest.DecodeFrom(key);

    s = builder->Finish();
    if (s.ok()) {
      meta->file_size = builder->FileSize();
    }
    delete builder;

    if (s.ok()) {
      s = file->Sync();
    }
    if (s.ok()) {
      s = file->Close();
    }
    delete file;
    file = nullptr;
  }
  if (!iter->status().ok()) {
    s = iter->status();
  }
  if (!s.ok() || meta->file_size == 0) {
    option.env->RemoveFile(filename);
  }
  return s;
}
struct SSTableBuilder::Rep {
  Rep(const Option& option, WritableFile* file)
      : closed_(false),
        data_block_option_(option),
        index_block_option_(option),
        file_(file),
        data_block_builder_(&data_block_option_),
        index_block_builder_(&index_block_option_),
        filter_block_builder_(
            option.filter_policy == nullptr
                ? nullptr
                : new FilterBlockBuilder(option.filter_policy)),
        num_entries_(0),
        offset_(0),
        data_block_over_(false) {
    // index_block is used for random access
    // using prefix compress will slow down the effiency.
    index_block_option_.block_restart_interval = 1;
  }
  bool closed_;
  Status status_;
  Option data_block_option_;
  Option index_block_option_;
  WritableFile* file_;
  BlockBuilder data_block_builder_;
  BlockBuilder index_block_builder_;
  FilterBlockBuilder* filter_block_builder_;
  std::string last_key_;
  uint64_t num_entries_;
  uint64_t offset_;
  // Set when a data block is flushed.
  // Used for index block building.
  bool data_block_over_;
  BlockHandle data_block_handle_;
  std::string compress_output_;
};
Status SSTableBuilder::status() const { return rep_->status_; }
SSTableBuilder::SSTableBuilder(const Option& option, WritableFile* file)
    : rep_(new Rep(option, file)) {
  if (rep_->filter_block_builder_ != nullptr) {
    rep_->filter_block_builder_->StartBlock(0);
  }
}

SSTableBuilder::~SSTableBuilder() {
  delete rep_->filter_block_builder_;
  delete rep_;
}

void SSTableBuilder::Add(std::string_view key, std::string_view value) {
  assert(!rep_->closed_);
  if (!ok()) return;
  if (rep_->num_entries_ != 0) {
    assert(rep_->data_block_option_.comparator->Compare(
               key, std::string_view(rep_->last_key_)) > 0);
  }
  // When a data block is flushed over,
  // Add a record into the index block
  // key : a key greater than last data block's last key,
  //       smaller than curr data block's first key
  // value : the block handle of last data block
  if (rep_->data_block_over_) {
    // find the shortest key between last_key and key
    // in order to decrease the key's length
    rep_->data_block_option_.comparator->FindShortestMiddle(&rep_->last_key_,
                                                            key);
    std::string block_handele;
    rep_->data_block_handle_.EncodeTo(&block_handele);
    rep_->index_block_builder_.Add(rep_->last_key_,
                                   std::string_view(block_handele));
    rep_->data_block_over_ = false;
  }
  if (rep_->filter_block_builder_ != nullptr) {
    rep_->filter_block_builder_->AddKey(key);
  }
  rep_->data_block_builder_.Add(key, value);
  rep_->last_key_ = key;
  rep_->num_entries_++;

  if (rep_->data_block_builder_.ByteSize() >=
      rep_->data_block_option_.block_size) {
    Flush();
  }
}

void SSTableBuilder::Flush() {
  assert(!rep_->closed_);
  if (rep_->data_block_builder_.Empty()) return;
  WriteBlock(&rep_->data_block_builder_, &rep_->data_block_handle_);
  if (ok()) {
    rep_->data_block_over_ = true;
    rep_->status_ = rep_->file_->Flush();
  }
  if (rep_->filter_block_builder_ != nullptr) {
    rep_->filter_block_builder_->StartBlock(rep_->offset_);
  }
}

// check the snappy compress is used.
// generate the std::string_view contents
void SSTableBuilder::WriteBlock(BlockBuilder* builder, BlockHandle* handle) {
  assert(ok());
  std::string_view contents;
  std::string_view raw = builder->Finish();
  CompressType type = rep_->data_block_option_.compress_type;

  if (type == KUnCompress) {
    contents = raw;
  } else if (type == KSnappyCompress) {
    std::string* compress = &rep_->compress_output_;
    if (snappy::Compress(raw.data(), raw.size(), compress)) {
      contents = *compress;
    } else {
      contents = raw;
      type = KUnCompress;
    }
  }
  WriteRawBlock(contents, type, handle);
  rep_->compress_output_.clear();
  builder->Reset();
}

void SSTableBuilder::WriteRawBlock(std::string_view contents, CompressType type,
                                   BlockHandle* handle) {
  handle->SetOffset(rep_->offset_);
  handle->SetSize(contents.size());
  rep_->status_ = rep_->file_->Append(contents);
  if (ok()) {
    char tail[KBlockTailSize];
    tail[0] = static_cast<char>(type);
    uint32_t crc = crc32c::Crc32c(contents.data(), contents.size());
    EncodeFixed32(tail + 1, CrcMask(crc));
    crc = crc32c::Extend(crc, reinterpret_cast<const uint8_t*>(tail), 1);
    rep_->status_ = rep_->file_->Append(std::string_view(tail, KBlockTailSize));
    if (ok()) {
      rep_->offset_ += contents.size() + KBlockTailSize;
    }
  }
}

Status SSTableBuilder::Finish() {
  assert(!rep_->closed_);
  Flush();
  rep_->closed_ = true;
  BlockHandle filter_block_handle, index_block_handle, filter_index_handle;
  if (ok() && rep_->filter_block_builder_ != nullptr) {
    WriteRawBlock(rep_->filter_block_builder_->Finish(), KUnCompress,
                  &filter_block_handle);
  }
  if (ok()) {
    BlockBuilder filter_index_builder(&rep_->data_block_option_);
    if (rep_->filter_block_builder_ != nullptr) {
      std::string key = "filter";
      key.append(rep_->data_block_option_.filter_policy->Name());
      std::string val;
      filter_block_handle.EncodeTo(&val);
      filter_index_builder.Add(key, val);
    }
    WriteBlock(&filter_index_builder, &filter_index_handle);
  }
  if (ok()) {
    if (rep_->data_block_over_) {
      // find the shortest key bigger than last_key
      // in order to decrease the key's length
      rep_->data_block_option_.comparator->FindShortestBigger(&rep_->last_key_);
      std::string block_handele;
      rep_->data_block_handle_.EncodeTo(&block_handele);
      rep_->index_block_builder_.Add(rep_->last_key_,
                                     std::string_view(block_handele));
      rep_->data_block_over_ = false;
    }
    WriteBlock(&rep_->index_block_builder_, &index_block_handle);
  }
  if (ok()) {
    Footer footer;
    footer.SetIndexHandle(index_block_handle);
    footer.SetFilterHandle(filter_block_handle);
    std::string footer_encode;
    footer.EncodeTo(&footer_encode);
    rep_->status_ = rep_->file_->Append(footer_encode);
    if (ok()) {
      rep_->offset_ += footer_encode.size();
    }
    {}
  }
  return rep_->status_;
}

uint64_t SSTableBuilder::FileSize() const { return rep_->offset_; }

uint64_t SSTableBuilder::NumEntries() const { return rep_->num_entries_; }

}  // namespace lsmkv