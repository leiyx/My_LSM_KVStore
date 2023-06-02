#ifndef STORAGE_XDB_INCLUDE_SSTABLE_READER_H_
#define STORAGE_XDB_INCLUDE_SSTABLE_READER_H_

#include "include/option.h"
#include "util/file.h"
namespace lsmkv {

class Footer;
class Iterator;

class SSTableReader {
 public:
    ~SSTableReader();

    SSTableReader(const SSTableReader&) = delete;
    SSTableReader operator=(const SSTableReader&) = delete;
    
    static Status Open(const Option& option, RandomReadFile* file,
        uint64_t file_size, SSTableReader** table);

    Iterator* NewIterator(const ReadOption& option) const;
 private:
    struct Rep;
    friend class TableCache;

    explicit SSTableReader(Rep* rep) : rep_(rep) {}

    void ReadFilterIndex(const Footer& footer);

    void ReadFilter(std::string_view handle_contents);

    Status InternalGet(const ReadOption& option, std::string_view key, void* arg,
             void (*handle_result)(void*, std::string_view, std::string_view));
    
    static Iterator* ReadBlockHandle(void* arg, const ReadOption& option, std::string_view handle_contents);
    
    Rep* const rep_;
};

}

#endif // STORAGE_XDB_INCLUDE_SSTABLE_READER_H_