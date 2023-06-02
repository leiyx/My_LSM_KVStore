#include <cassert>
#include <algorithm>

#include "db/sstable/block_builder.h"
#include "util/coding.h"
namespace lsmkv {

BlockBuilder::BlockBuilder(const Option* option)
    : option_(option), restarts_(), counter_(0), finished_(false) {
        restarts_.push_back(0);
    }
void BlockBuilder::Add(std::string_view key, std::string_view value) {
    assert(!finished_);
    std::string_view last_key(last_key_);
    size_t shared = 0;
    if (counter_ < option_->block_restart_interval) {
        size_t min_size = std::min(key.size(), last_key.size());
        while ((shared < min_size) && (key[shared] == last_key[shared])) {
            shared++;
        }
    } else {
        restarts_.push_back(buffer_.size());
        counter_ = 0;
    }
    size_t non_shard = key.size() - shared;
    PutVarint32(&buffer_, shared);
    PutVarint32(&buffer_, non_shard);
    PutVarint32(&buffer_, value.size());

    buffer_.append(key.data() + shared, non_shard);
    buffer_.append(value.data(), value.size());
    last_key_.resize(shared);
    last_key_.append(key.data() + shared, non_shard);
    counter_++;
}

std::string_view BlockBuilder::Finish() {
    for (uint32_t restart : restarts_) {
        PutFixed32(&buffer_, restart);
    }
    PutFixed32(&buffer_, restarts_.size());
    finished_ = true;
    return std::string_view(buffer_);
}

size_t BlockBuilder::ByteSize() const {
    if (finished_) {
        return buffer_.size();
    } else {
        return (buffer_.size() +                        // actual key - value
                restarts_.size() * sizeof(uint32_t) +   // restarts_
                sizeof(uint32_t));                      // restart num
    }
}

void BlockBuilder::Reset() {
    buffer_.clear();
    restarts_.clear();
    counter_ = 0;
    last_key_.clear();
    finished_ = false;
    restarts_.push_back(0);
}

}