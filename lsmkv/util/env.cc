#include "include/env.h"

#include <dirent.h>
#include <fcntl.h>

#include <cstring>
#include <set>

#include "util/logger.h"
// allow mmap file if 64bits because of enough address capacity.

namespace lsmkv {

constexpr const int max_mmap_limit = (sizeof(void*) >= 8) ? 1000 : 0;

void Log(Logger* logger, const char* format, ...) {
  if (logger != nullptr) {
    std::va_list ap;
    va_start(ap, format);
    logger->Logv(format, ap);
    va_end(ap);
  }
}

class FileLockImpl : public FileLock {
 public:
  FileLockImpl(int fd, std::string filename)
      : fd_(fd), filename_(std::move(filename)) {}
  const int fd_;
  const std::string filename_;
};

class LockTable {
 public:
  bool Insert(const std::string& filename) {
    MutexLock l(&mu_);
    return locked_files_.insert(filename).second;
  }
  void Delete(const std::string& filename) {
    MutexLock l(&mu_);
    locked_files_.erase(filename);
  }

 private:
  Mutex mu_;
  std::set<std::string> locked_files_ GUARDED_BY(mu_);
};
class EnvImpl : public Env {
 public:
  EnvImpl()
      : background_work_cv_(&background_work_mutex_),
        background_work_doing_(false),
        mmap_limiter_(max_mmap_limit),
        pread_limiter_(MaxOpenFiles()) {}

  Status NewSequentialFile(const std::string& filename,
                           SequentialFile** result) override {
    int fd = ::open(filename.data(), O_RDONLY);
    if (fd < 0) {
      *result = nullptr;
      return SystemError(filename, errno);
    }
    *result = new SequentialFile(filename, fd);
    return Status::OK();
  }

  Status NewRamdomReadFile(const std::string& filename,
                           RandomReadFile** result) override {
    int fd = ::open(filename.data(), O_RDONLY);
    if (fd < 0) {
      *result = nullptr;
      return SystemError(filename, errno);
    }

    if (!mmap_limiter_.Acquire()) {
      *result = new PreadRamdomReadFile(filename, &pread_limiter_, fd);
      return Status::OK();
    }
    uint64_t file_size;
    Status status = FileSize(filename, &file_size);
    if (status.ok()) {
      void* mmap_base = ::mmap(0, file_size, PROT_READ, MAP_SHARED, fd, 0);
      if (mmap_base == MAP_FAILED) {
        status = SystemError(filename, errno);
      } else {
        *result = new MmapRandomReadFile(
            filename, static_cast<char*>(mmap_base), file_size, &mmap_limiter_);
      }
    }
    ::close(fd);
    if (!status.ok()) {
      mmap_limiter_.Release();
    }
    return status;
  }

  Status NewWritableFile(const std::string& filename,
                         WritableFile** result) override {
    int fd = ::open(filename.data(), O_TRUNC | O_WRONLY | O_CREAT, 0644);
    if (fd < 0) {
      *result = nullptr;
      return SystemError(filename, errno);
    }
    *result = new WritableFile(filename, fd);
    return Status::OK();
  }

  Status NewAppendableFile(const std::string& filename,
                           WritableFile** result) override {
    int fd = ::open(filename.data(), O_APPEND | O_WRONLY | O_CREAT, 0644);
    if (fd < 0) {
      *result = nullptr;
      return SystemError(filename, errno);
    }
    *result = new WritableFile(filename, fd);
    return Status::OK();
  }

  Status CreatDir(const std::string& dirname) override {
    if (::mkdir(dirname.data(), 0755) != 0) {
      return SystemError(dirname, errno);
    }
    return Status::OK();
  }

  Status RemoveDir(const std::string& dirname) override {
    if (::rmdir(dirname.data()) != 0) {
      return SystemError(dirname, errno);
    }
    return Status::OK();
  }
  Status GetChildren(const std::string& dirname,
                     std::vector<std::string>* filenames) override {
    filenames->clear();
    ::DIR* dir = ::opendir(dirname.data());
    if (dir == nullptr) {
      return SystemError(dirname, errno);
    }
    ::dirent* entry;
    while ((entry = ::readdir(dir)) != nullptr) {
      filenames->emplace_back(entry->d_name);
    }
    ::closedir(dir);
    return Status::OK();
  }

  Status RenameFile(const std::string& from, const std::string& to) override {
    if (::rename(from.data(), to.data()) != 0) {
      return SystemError(from, errno);
    }
    return Status::OK();
  }

  Status FileSize(const std::string& filename, uint64_t* result) override {
    struct ::stat file_stat;
    if (::stat(filename.data(), &file_stat) < 0) {
      *result = 0;
      return SystemError(filename, errno);
    }
    *result = file_stat.st_size;
    return Status::OK();
  }

  Status RemoveFile(const std::string& filename) override {
    if (::unlink(filename.data()) < 0) {
      return SystemError(filename, errno);
    }
    return Status::OK();
  }

  bool FileExist(const std::string& filename) override {
    return ::access(filename.c_str(), F_OK) == 0;
  }
  Status LockFile(const std::string& filename, FileLock** lock) override {
    *lock = nullptr;
    int fd = ::open(filename.data(), O_CREAT | O_RDWR, 0644);
    if (fd < 0) {
      return SystemError(filename, errno);
    }
    if (!locks_.Insert(filename)) {
      ::close(fd);
      return Status::IOError("lock" + filename, "already be held");
    }
    if (LockOrUnlockFile(fd, true) == -1) {
      int lock_errno = errno;
      ::close(fd);
      locks_.Delete(filename);
      return SystemError("lock" + filename, errno);
    }
    *lock = new FileLockImpl(fd, filename);
    return Status::OK();
  }

  Status UnlockFile(FileLock* lock) override {
    FileLockImpl* lock_impl = static_cast<FileLockImpl*>(lock);
    if (LockOrUnlockFile(lock_impl->fd_, false) == -1) {
      return SystemError("lock" + lock_impl->filename_, errno);
    }
    locks_.Delete(lock_impl->filename_);
    ::close(lock_impl->fd_);
    delete lock_impl;
    return Status::OK();
  }

  void Schedule(void (*function)(void* arg), void* arg) override;

  void StartThread(void (*function)(void* arg), void* arg) override {
    std::thread new_thread(function, arg);
    new_thread.detach();
  }
  void SleepMicroseconds(int n) override {
    std::this_thread::sleep_for(std::chrono::microseconds(n));
  }

  Status NewLogger(const std::string& filename, Logger** result) override {
    int fd = ::open(filename.data(), O_CREAT | O_RDWR, 0644);
    if (fd < 0) {
      result = nullptr;
      return SystemError(filename, errno);
    }
    std::FILE* fp = ::fdopen(fd, "w");
    if (fp == nullptr) {
      ::close(fd);
      *result = nullptr;
      return SystemError(filename, errno);
    } else {
      *result = new LoggerImpl(fp);
      return Status::OK();
    }
  }

 private:
  int LockOrUnlockFile(int fd, bool is_lock) {
    errno = 0;
    ::flock lock_info;
    std::memset(&lock_info, 0, sizeof(lock_info));
    lock_info.l_type = (is_lock ? F_WRLCK : F_ULOCK);
    lock_info.l_whence = SEEK_SET;
    lock_info.l_start = 0;
    lock_info.l_len = 0;
    return ::fcntl(fd, F_SETLK, &lock_info);
  }
  void BackgroundThreadMain();

  static void BackgroundThreadMainEntry(EnvImpl* env) {
    env->BackgroundThreadMain();
  }

  int MaxOpenFiles() {
    struct ::rlimit rlim;
    int max_open_files;
    if (::getrlimit(RLIMIT_NOFILE, &rlim) < 0) {
      max_open_files = 50;
    } else if (rlim.rlim_cur == RLIM_INFINITY) {
      max_open_files = std::numeric_limits<int>::max();
    } else {
      max_open_files = rlim.rlim_cur / 5;
    }
    return max_open_files;
  }

  struct BackgroundWorkItem {
    explicit BackgroundWorkItem(void (*function)(void* arg), void* arg)
        : function_(function), arg_(arg) {}
    void (*const function_)(void*);
    void* const arg_;
  };

  Mutex background_work_mutex_;
  CondVar background_work_cv_;
  std::queue<BackgroundWorkItem> background_work_queue_
      GUARDED_BY(background_work_mutex_);
  bool background_work_doing_ GUARDED_BY(background_work_mutex_);
  Limiter mmap_limiter_;
  Limiter pread_limiter_;
  LockTable locks_;
};

void EnvImpl::BackgroundThreadMain() {
  while (true) {
    background_work_mutex_.Lock();
    while (background_work_queue_.empty()) {
      background_work_cv_.Wait();
    }
    BackgroundWorkItem item = background_work_queue_.front();
    auto function = item.function_;
    void* arg = item.arg_;
    background_work_queue_.pop();

    background_work_mutex_.Unlock();
    function(arg);
  }
}

void EnvImpl::Schedule(void (*function)(void* arg), void* arg) {
  background_work_mutex_.Lock();
  if (!background_work_doing_) {
    background_work_doing_ = true;
    std::thread background_thread(EnvImpl::BackgroundThreadMainEntry, this);
    background_thread.detach();
  }
  if (background_work_queue_.empty()) {
    background_work_cv_.Signal();
  }
  background_work_queue_.emplace(function, arg);
  background_work_mutex_.Unlock();
}

class SingletonDefaultEnv {
 public:
  SingletonDefaultEnv() { new (storage_) EnvImpl; }
  Env* Get() { return reinterpret_cast<Env*>(storage_); }

 private:
  alignas(EnvImpl) char storage_[sizeof(EnvImpl)];
};

Env* DefaultEnv() {
  static SingletonDefaultEnv singleton;
  return singleton.Get();
}

}  // namespace lsmkv