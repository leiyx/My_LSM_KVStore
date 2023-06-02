#ifndef STORAGE_XDB_UTIL_MUTEX_H_
#define STORAGE_XDB_UTIL_MUTEX_H_

#include <mutex>
#include <condition_variable>
#include <cassert>

#include "util/thread_annotations.h"
namespace lsmkv {

class CondVar;

class LOCKABLE Mutex {
 public:
    Mutex() = default;
    ~Mutex() = default;
    Mutex(const Mutex&) = delete;
    Mutex& operator=(const Mutex&) = delete;

    void Lock() EXCLUSIVE_LOCK_FUNCTION() { mu_.lock(); }
    void Unlock() UNLOCK_FUNCTION() { mu_.unlock(); }
    void AssertHeld() ASSERT_EXCLUSIVE_LOCK() {}
 private:
    friend class CondVar;
    std::mutex mu_;
};

class CondVar {
 public:
   explicit CondVar(Mutex* mu) : mu_(mu) {
        assert( mu != nullptr);
   }
   ~CondVar() = default;
   CondVar(const CondVar&) = delete;
   CondVar& operator=(const CondVar&) = delete;

   void Wait() {
      std::unique_lock<std::mutex> l(mu_->mu_, std::adopt_lock);
      cv_.wait(l);
      l.release();
   }
   void Signal() { cv_.notify_one(); }
   void SignalAll() { cv_.notify_all(); }
 private:
    std::condition_variable cv_;
    Mutex* const mu_;
};

class SCOPED_LOCKABLE MutexLock {
 public:
   explicit MutexLock(Mutex* mu) EXCLUSIVE_LOCK_FUNCTION(mu)
      : mu_(mu) { mu_->Lock(); }
   ~MutexLock() UNLOCK_FUNCTION() { mu_->Unlock(); }
 private:
   Mutex* const mu_;
};

}

#endif // STORAGE_XDB_UTIL_MUTEX_H_