#ifndef STORAGE_XDB_INCLUDE_ENV_H_
#define STORAGE_XDB_INCLUDE_ENV_H_

#include <sys/resource.h>
#include <sys/stat.h>
#include <limits>
#include <thread>
#include <queue>
#include <vector>
#include <cstdarg>

#include "util/mutex.h"
#include "util/file.h"

// allow mmap file if 64bits because of enough address capacity.

namespace lsmkv {

class FileLock {
 public:
    FileLock() = default;
    FileLock(const FileLock&) = delete;
    FileLock& operator=(const FileLock&) = delete;
    virtual ~FileLock() = default;
};

class Logger {
 public:
    Logger() = default;
    Logger(const Logger&) = delete;
    Logger& operator=(const Logger&) = delete;
    virtual ~Logger() = default;

    virtual void Logv(const char* format, std::va_list ap) = 0;
};

class Env {
 public:
    Env() = default;
    
    virtual Status NewSequentialFile(const std::string& filename, SequentialFile** result) = 0;

    virtual Status NewRamdomReadFile(const std::string& filename, RandomReadFile** result) = 0;

    virtual Status NewWritableFile(const std::string& filename, WritableFile** result) = 0;

    virtual Status NewAppendableFile(const std::string& filename, WritableFile** result) = 0;

    virtual Status CreatDir(const std::string& filename) = 0;

    virtual Status RemoveDir(const std::string& filename) = 0;

    virtual Status GetChildren(const std::string& dirname, std::vector<std::string>* filenames) = 0;

    virtual Status FileSize(const std::string& filename, uint64_t* result) = 0;

    virtual Status RenameFile(const std::string& from, const std::string& to) = 0;

    virtual Status RemoveFile(const std::string& filename) = 0;

    virtual bool FileExist(const std::string& filename) = 0;

    virtual Status LockFile(const std::string& filename, FileLock** lock) = 0;
    
    virtual Status UnlockFile(FileLock* lock) = 0;

    virtual void Schedule(void (*function)(void *arg), void* arg) = 0;

    virtual void StartThread(void (*function)(void *arg), void* arg) = 0;

    virtual void SleepMicroseconds(int n) = 0;

    virtual Status NewLogger(const std::string& filename, Logger** result) = 0;
};

void Log(Logger* logger, const char* format, ...);

Env* DefaultEnv();

}
#endif // STORAGE_XDB_INCLUDE_ENV_H_