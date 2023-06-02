#ifndef STORAGE_XDB_DB_LOG_LOG_WRITER_H_
#define STORAGE_XDB_DB_LOG_LOG_WRITER_H_

#include <cstdint>

#include <string_view>
#include "include/status.h"
#include "db/log/log_format.h"

namespace lsmkv {

class WritableFile;

namespace log {

class Writer {
 public:
    explicit Writer(WritableFile* dest);

    // initialize the writer with file "dest" not empty
    // which has "dest_length" length.
    Writer(WritableFile* dest, uint64_t dest_length);

    Writer(const Writer&) = delete;
    Writer& operator=(const Writer&) = delete;

    ~Writer() = default;

    Status AddRecord(std::string_view sv);
 private:
    Status WritePhysicalRecord(LogRecodeType type, const char* p, size_t len);
    WritableFile* dest_;
    size_t block_offset_;
    uint32_t type_crc_[KMaxType + 1];
};

};

}

#endif // STORAGE_XDB_DB_LOG_LOG_WRITER_H_