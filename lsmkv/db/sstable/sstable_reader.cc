#include "include/sstable_reader.h"
#include "db/filter/filter_block.h"
#include "db/sstable/block_reader.h"
#include "db/sstable/block_format.h"
#include "util/file.h"

namespace lsmkv {


struct SSTableReader::Rep {
    ~Rep() {
        delete filter;
        delete[] filter_data;
        delete index_block;
    }
    Option option;
    Status status;
    RandomReadFile* file;
    BlockReader* index_block;

    const char* filter_data;
    FilterBlockReader* filter;
};

SSTableReader::~SSTableReader() {
    delete rep_;
}

Status SSTableReader::Open(const Option& option, RandomReadFile* file,
        uint64_t file_size, SSTableReader** table) {
    // read footer from file, include the handle of 
    // index block and filter index block
    *table = nullptr;
    if (file_size < Footer::KEncodeLength) {
        return Status::Corruption("file is too short to be sstable");
    }
    char footer_buffer[Footer::KEncodeLength];
    std::string_view footer_result;
    Status s = file->Read(file_size - Footer::KEncodeLength, Footer::KEncodeLength
        ,&footer_result,footer_buffer);
    if(!s.ok()) return s;
    Footer footer;
    s = footer.DecodeFrom(&footer_result);
    if(!s.ok()) return s;
    // read index block from file
    ReadOption read_option;
    if (option.check_crc) {
        read_option.check_crc = true;
    }
    BlockContents index_block_contents;
    s = ReadBlock(read_option, file, footer.GetIndexHandle(), &index_block_contents);
    if(!s.ok()) return s;

    BlockReader* index_block = new BlockReader(index_block_contents);
    Rep* rep = new SSTableReader::Rep;
    rep->option = option;
    rep->file = file;
    rep->index_block = index_block;
    rep->filter_data = nullptr;
    rep->filter = nullptr;
    *table = new SSTableReader(rep);
    (*table)->ReadFilterIndex(footer);
    return s;
}

void SSTableReader::ReadFilterIndex(const Footer& footer) {
    if (rep_->option.filter_policy == nullptr) {
        return;
    }
    ReadOption read_option;
    if (rep_->option.check_crc) {
        read_option.check_crc = true;
    }
    BlockContents filter_index_contents;
    if (!ReadBlock(read_option,rep_->file, footer.GetFilterHandle(),
            &filter_index_contents).ok()) {
        return;
    }

    BlockReader* filter_index_block = new BlockReader(filter_index_contents);
    Iterator* iter = filter_index_block->NewIterator(DefaultComparator());
    std::string key = "filter";
    key.append(rep_->option.filter_policy->Name());
    iter->Seek(key);

    if (iter->Valid() && iter->Key() == std::string_view(key)) {
        ReadFilter(iter->Value());
    }

    delete filter_index_block;
    delete iter;
}

void SSTableReader::ReadFilter(std::string_view handle_contents) {
    std::string_view s = handle_contents;
    BlockHandle filter_handle;
    if(!filter_handle.DecodeFrom(&s).ok()) {
        return;
    }
    ReadOption read_option;
    if (rep_->option.check_crc) {
        read_option.check_crc = true;
    }
    BlockContents filter_contents;
    if (!ReadBlock(read_option,rep_->file, filter_handle,
            &filter_contents).ok()) {
        return;
    }
    if (!filter_contents.heap_allocated_) {
        rep_->filter_data = filter_contents.data.data();
    }
    rep_->filter = new FilterBlockReader(rep_->option.filter_policy, filter_contents.data);
}

static void DeleteBlock(void* arg, void* none) {
    delete reinterpret_cast<BlockReader*>(arg);
}
Iterator* SSTableReader::ReadBlockHandle(void* arg, const ReadOption& option, std::string_view handle_contents) {
    SSTableReader* table = reinterpret_cast<SSTableReader*>(arg);
    BlockHandle handle;
    BlockReader* block = nullptr;
    BlockContents block_contents;
    std::string_view tmp = handle_contents;
    Status s = handle.DecodeFrom(&tmp);

    if (s.ok()) {
        s = ReadBlock(option, table->rep_->file, handle, &block_contents);
        if (s.ok()) {
            block = new BlockReader(block_contents);
        }
    }

    Iterator* iter;
    if (block == nullptr) {
        iter = NewErrorIterator(s);
    } else {
        iter = block->NewIterator(table->rep_->option.comparator);
        iter->AppendCleanup(&DeleteBlock, block, nullptr);
    }

    return iter;
}

Iterator* SSTableReader::NewIterator(const ReadOption& option) const {
    return NewTwoLevelIterator(
        rep_->index_block->NewIterator(rep_->option.comparator),
        &ReadBlockHandle, const_cast<SSTableReader*>(this), option
    );
}

Status SSTableReader::InternalGet(const ReadOption& option, std::string_view key, void* arg,
             void (*handle_result)(void*, std::string_view, std::string_view)) {
    Status s;
    Iterator* index_iter = rep_->index_block->NewIterator(rep_->option.comparator);
    index_iter->Seek(key);
    if (index_iter->Valid()) {
        std::string_view handle_content = index_iter->Value();
        BlockHandle handle;
        FilterBlockReader* filter = rep_->filter;
        if (filter != nullptr && handle.DecodeFrom(&handle_content).ok() 
                && filter->KeyMayMatch(handle.GetOffset(), key)) {
            // key is not found.
        } else {
            Iterator* block_iter = ReadBlockHandle(this, option, handle_content);
            block_iter->Seek(key);
            if (block_iter->Valid()) {
                (*handle_result)(arg, block_iter->Key(), block_iter->Value());
            }
            s = block_iter->status();
            delete block_iter;
        }
    }
    if (s.ok()) {
        s = index_iter->status();
    }
    delete index_iter;
    return s;
}

}

