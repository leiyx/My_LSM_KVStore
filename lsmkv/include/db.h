#ifndef STORAGE_XDB_INCLUDE_DB_H_
#define STORAGE_XDB_INCLUDE_DB_H_

#include "include/status.h"
#include "include/writebatch.h"
#include "include/option.h"
namespace lsmkv {

class DB {
 public:
    virtual ~DB() = default;

    static Status Open(const Option& option, const std::string& name, DB** ptr);

    virtual Status Get(const ReadOption& option,std::string_view key, std::string* value) = 0;

    virtual Status Put(const WriteOption& option, std::string_view key, std::string_view value) = 0;

    virtual Status Delete(const WriteOption& option,std::string_view key) = 0;

    virtual Status Write(const WriteOption& option,WriteBatch* batch) = 0;
};

Status DestoryDB(const Option& option, const std::string& name);
}
#endif // STORAGE_XDB_INCLUDE_DB_H