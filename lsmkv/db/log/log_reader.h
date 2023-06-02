#ifndef STORAGE_XDB_DB_LOG_LOG_READER_H_
#define STORAGE_XDB_DB_LOG_LOG_READER_H_

#include <cstdint>

#include <string_view>
#include "include/status.h"
#include "db/log/log_format.h"

namespace lsmkv {
class SequentialFile;

namespace log {

class Reader {
 public:
    Reader(SequentialFile* src, bool checksum, uint64_t initial_offset);
    
    ~Reader();
    
    bool ReadRecord(std::string_view* record, std::string* buffer);

    uint64_t LastRecordOffset() { return last_record_offset_; }
 private:
    enum {
        // KEof : finish the record reading.
        KEof = KMaxType + 1,
        // KBadRecord : ignore the record and read next.
        KBadRecord = KMaxType + 2
    };
    unsigned int ReadPhysicalRecord(std::string_view* fragments);

    SequentialFile* src_;
    const bool checksum_;
    const uint64_t initial_offset_;
    std::string_view buffer_;
    char* const buffer_mem_;
    bool eof_;

    uint64_t last_record_offset_;
    uint64_t buffer_end_offset_;
};

};

}

#endif // STORAGE_XDB_DB_LOG_LOG_READER_H_