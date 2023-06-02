#ifndef STORAGE_XDB_INCLUDE_OPTION_H_
#define STORAGE_XDB_INCLUDE_OPTION_H_

#include "include/env.h"
namespace lsmkv {

class Comparator;
class Env;
class FilterPolicy;

enum CompressType {
    KUnCompress = 0,
    KSnappyCompress = 1
};

struct Option {
    Option();

    // the method to compare two user keys.
    // default:: by byte wise compare
    const Comparator* comparator;

    // when access a record, use this is helpful to check if the
    // record is possible in the storage.It is helpful to reduce 
    // the disk.
    const FilterPolicy* filter_policy = nullptr;

    // Compress the sstable use the compression algorithm.
    CompressType compress_type = KSnappyCompress;
    
    // include the methods that interact with the os.
    // such as start thread, open file, lock file, etc.
    Env* env;

    // the numbers N of key between with two restarts
    // restarts : the keys of block are prefix compressed.
    //            For speed up the random access, every N 
    //            keys is stored a full key.
    int block_restart_interval = 16;

    // the approximate size of block(unit of sstable) that 
    // store the user data. A access to SSTable will return
    // a block_size memory.
    // default : 4KB
    size_t block_size = 4 * 1024;

    // if true, while check the block of sstable is whether complete.
    // when one record is break, it may cause all the block is unseenable. 
    bool check_crc = false;

    // when memtable's size greater than this,
    // memtable will be writen to disk as SStable.
    // default : 4MB
    size_t write_mem_size = 4 * 1024 * 1024; 

    // Numbers of open files that can be used by db.
    int max_open_file = 1000;

    // the error/progress information will be written to logger
    Logger* logger = nullptr;

    // write up to this amount bytes to a file before switch
    uint64_t max_file_size = 2 * 1024 * 1024;
};

struct WriteOption {
    // if true, the log of write will be recorded to os before
    // the write is considered over.
    bool sync = false;
};

struct ReadOption {
    // if true, the data are readed will be checked the completeness
    // when read from files.
    bool check_crc = false;
};

}

#endif // STORAGE_XDB_INCLUDE_OPTION_H_