#ifndef THREADPOOL_H
#define THREADPOOL_H

#include <atomic>
#include <condition_variable>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <queue>
#include <stdexcept>
#include <thread>
#include <vector>

class ThreadPool {
 public:
  explicit ThreadPool(size_t);
  ~ThreadPool();
  template <class F, class... Args>
  auto Enqueue(F&& f, Args&&... args)
      -> std::future<typename std::result_of<F(Args...)>::type>;

 private:
  // 一组工作线程
  std::vector<std::thread> workers_;
  // 任务队列
  std::queue<std::function<void()> > tasks_;
  // 停止标志
  std::atomic<bool> stop_;
  // 同步相关
  std::mutex mutex_;
  std::condition_variable cond_;
};

/**
 * @brief 线程池构造
 * @details 线程池构造时，工作线程就已经启动
 * @param[in] thread_num 线程数量
 */
inline ThreadPool::ThreadPool(size_t thread_num) : stop_(false) {
  for (size_t i = 0; i < thread_num; ++i)
    workers_.emplace_back([this] {
      while (true) {
        std::function<void()> task;

        {
          std::unique_lock<std::mutex> lock(this->mutex_);
          this->cond_.wait(lock, [this] {
            return this->stop_.load() || !this->tasks_.empty();
          });
          if (this->stop_.load() && this->tasks_.empty()) return;
          task = std::move(this->tasks_.front());
          this->tasks_.pop();
        }

        task();
      }
    });
}

inline ThreadPool::~ThreadPool() {
  stop_.store(true);
  cond_.notify_all();

  for (std::thread& worker : workers_) worker.join();
}

/**
 * @brief 向线程池中添加任务
 */
template <class F, class... Args>
auto ThreadPool::Enqueue(F&& f, Args&&... args)
    -> std::future<typename std::result_of<F(Args...)>::type> {
  using return_type = typename std::result_of<F(Args...)>::type;

  auto task = std::make_shared<std::packaged_task<return_type()> >(
      std::bind(std::forward<F>(f), std::forward<Args>(args)...));

  std::future<return_type> res = task->get_future();
  {
    std::unique_lock<std::mutex> lock(mutex_);

    // 当stop标志为true时，不能再添加任务
    if (stop_.load())
      throw std::runtime_error("KVStore has been closed, cannot add task");

    tasks_.emplace([task]() { (*task)(); });
  }
  cond_.notify_one();
  return res;
}

#endif  // THREADPOOL_H