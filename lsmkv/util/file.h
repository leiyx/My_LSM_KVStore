#ifndef STORAGE_XDB_UTIL_FILE_H_
#define STORAGE_XDB_UTIL_FILE_H_

#include <fcntl.h>
#include <sys/mman.h>
#include <unistd.h>

#include <atomic>
#include <cassert>
#include <cstring>
#include <string>

#include "include/status.h"

namespace lsmkv {

constexpr const size_t KWritableFileBufferSize = 1 << 16;

static Status SystemError(const std::string& msg, int error_num) {
  if (error_num == ENOENT) {
    return Status::NotFound(msg, std::strerror(error_num));
  } else {
    return Status::IOError(msg, std::strerror(error_num));
  }
}

class Limiter {
 public:
  Limiter(int max_acquire) : acquire_remains_(max_acquire) {
    assert(max_acquire >= 0);
  }

  Limiter(const Limiter&) = delete;
  Limiter& operator=(const Limiter&) = delete;

  bool Acquire() {
    int old_acquire_remains =
        acquire_remains_.fetch_sub(1, std::memory_order_relaxed);
    if (old_acquire_remains > 0) return true;
    acquire_remains_.fetch_add(1, std::memory_order_relaxed);
    return false;
  }

  void Release() { acquire_remains_.fetch_add(1, std::memory_order_relaxed); }

 private:
  std::atomic<int> acquire_remains_;
};

class SequentialFile {
 public:
  SequentialFile(std::string filename, int fd)
      : fd_(fd), filename_(std::move(filename)) {}
  ~SequentialFile() { ::close(fd_); }

  Status Read(size_t n, std::string_view* result, char* buffer) {
    while (true) {
      ::ssize_t read_n = ::read(fd_, buffer, n);
      if (read_n < 0) {
        if (errno == EINTR) {
          continue;
        }
        return SystemError(filename_, errno);
      }
      *result = std::string_view(buffer, read_n);
      break;
    }
    return Status::OK();
  }

  Status Skip(uint64_t n) {
    if (::lseek(fd_, n, SEEK_CUR) == static_cast<off_t>(-1)) {
      return SystemError(filename_, errno);
    }
    return Status::OK();
  }

 private:
  const int fd_;
  const std::string filename_;
};

class RandomReadFile {
 public:
  RandomReadFile() = default;
  RandomReadFile(const RandomReadFile&) = delete;
  RandomReadFile& operator=(const RandomReadFile&) = delete;

  virtual ~RandomReadFile() = default;
  virtual Status Read(uint64_t offset, uint64_t n, std::string_view* result,
                      char* buffer) const = 0;
};

class MmapRandomReadFile : public RandomReadFile {
 public:
  MmapRandomReadFile(std::string filename, char* mmap_base, size_t length,
                     Limiter* limiter)
      : limiter_(limiter),
        filename_(std::move(filename)),
        length_(length),
        mmap_base_(mmap_base) {}
  ~MmapRandomReadFile() {
    ::munmap(static_cast<void*>(mmap_base_), length_);
    limiter_->Release();
  }

  Status Read(uint64_t offset, uint64_t n, std::string_view* result,
              char* buffer) const override {
    if (offset + n > length_) {
      *result = std::string_view();
      return SystemError(filename_, EINVAL);
    }
    *result = std::string_view(mmap_base_ + offset, n);
    return Status::OK();
  }

 private:
  Limiter* const limiter_;
  const std::string filename_;
  const size_t length_;
  char* const mmap_base_;
};

class PreadRamdomReadFile : public RandomReadFile {
 public:
  PreadRamdomReadFile(std::string filename, Limiter* limiter, int fd)
      : limiter_(limiter),
        filename_(std::move(filename)),
        has_fd_(limiter_->Acquire()),
        fd_(has_fd_ ? fd : -1) {}
  ~PreadRamdomReadFile() {
    if (has_fd_) {
      assert(fd_ != -1);
      ::close(fd_);
      limiter_->Release();
    }
  }

  Status Read(uint64_t offset, uint64_t n, std::string_view* result,
              char* buffer) const override {
    int fd = fd_;
    Status status;
    if (!has_fd_) {
      fd = ::open(filename_.data(), O_RDONLY);
      if (fd < 0) {
        return SystemError(filename_, errno);
      }
    }
    ssize_t read_size = ::pread(fd, buffer, n, static_cast<off_t>(offset));
    if (read_size < 0) {
      status = SystemError(filename_, errno);
    }
    if (!has_fd_) {
      ::close(fd);
    }
    *result = std::string_view(buffer, read_size);
    return status;
  }

 private:
  Limiter* const limiter_;
  const std::string filename_;
  const bool has_fd_;
  const int fd_;
};
class WritableFile {
 public:
  WritableFile(std::string filename, int fd)
      : pos_(0),
        fd_(fd),
        filename_(std::move(filename)),
        dirname_(Dirname(filename_)) {}
  ~WritableFile() {
    if (fd_ >= 0) {
      ::close(fd_);
    }
  }

  Status Append(std::string_view data) {
    size_t write_size = data.size();
    const char* write_data = data.data();

    // Append data to Buffer if buffer is enough
    size_t copy_size = std::min(write_size, KWritableFileBufferSize - pos_);
    std::memcpy(buffer_ + pos_, write_data, copy_size);
    write_data += copy_size;
    write_size -= copy_size;
    pos_ += copy_size;
    if (write_size == 0) {
      return Status::OK();
    }

    Status s = Flush();
    if (!s.ok()) {
      return s;
    }
    return WriteUnbuffer(write_data, write_size);
  }

  Status Flush() {
    Status status;
    status = WriteUnbuffer(buffer_, pos_);
    pos_ = 0;
    return status;
  }

  Status Sync() {
    Status status;
    status = Flush();
    if (!status.ok()) {
      return status;
    }
    return SyncFd(fd_, filename_);
  }

  Status SyncDir() {
    Status status;
    int fd;
    if ((fd = ::open(dirname_.data(), O_RDONLY)) < 0) {
      return SystemError(dirname_, errno);
    } else {
      status = SyncFd(fd, dirname_);
      ::close(fd);
    }
    return status;
  }

  Status Close() {
    Status status;
    status = Flush();
    if (!status.ok()) {
      return status;
    }
    if (::close(fd_) < 0) {
      return SystemError(filename_, errno);
    }
    fd_ = -1;
    return status;
  }

 private:
  std::string Dirname(const std::string& filename) const {
    auto seperator_pos = filename.rfind('/');
    if (seperator_pos == std::string::npos) {
      return std::string(".");
    }
    return filename.substr(0, seperator_pos);
  }
  Status SyncFd(int fd, const std::string& filename) {
    if (::fsync(fd) != 0) {
      return SystemError(filename_, errno);
    }
    return Status::OK();
  }
  Status WriteUnbuffer(const char* data, size_t size) {
    while (size > 0) {
      ssize_t write_size = ::write(fd_, data, size);
      if (write_size < 0) {
        if (errno == EINTR) {
          continue;
        }
        return SystemError(filename_, errno);
      }
      size -= write_size;
      data += write_size;
    }
    return Status::OK();
  }

  char buffer_[KWritableFileBufferSize];
  size_t pos_;
  int fd_;

  const std::string filename_;
  const std::string dirname_;
};

}  // namespace lsmkv
#endif  // STORAGE_XDB_INCLUDE_FILE_H_
