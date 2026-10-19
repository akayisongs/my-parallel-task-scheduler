#pragma once

#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <cstddef>
#include <stdexcept>

class ThreadPool {
private:
	// *类成员对象的初始化严格按照其在类定义中的声明顺序进行，而析构则严格按照声明顺序的逆序进行。*
	/* 状态与同步基础设施 */
	bool stop_{ false };
	std::mutex queue_mutex_;
	std::condition_variable cv_;

	/* 被保护的共享资源 */
	std::queue<std::function<void()>> tasks_;

	/* 执行实体 */
	std::vector<std::thread> workers_;

public:
	explicit ThreadPool(std::size_t threads) {
		workers_.reserve(threads);

		for (std::size_t i = 0;i < threads;i++) {
			workers_.emplace_back([this]() {
				while (1) {
					std::function<void()> task;// 实际工作任务接口

					{ // unique_lock作用域
						std::unique_lock lock(queue_mutex_);
						cv_.wait(lock, [this]() { return stop_ || !tasks_.empty();} );// 此处逻辑为当线程池未停止且队列为空返回0（为0则wait）

						if (stop_ && tasks_.empty()) { return; }

						task = std::move(tasks_.front());
						tasks_.pop();
					}

					task();
				}
				});
		}
	}
	~ThreadPool() {
		// unique_lock作用域
		{ std::unique_lock<std::mutex> lock(queue_mutex_);  stop_ = true; }
		cv_.notify_all();
		for (std::thread& worker : workers_) {
			if (worker.joinable()) { worker.join(); }
		}
	}

	// 禁用拷贝语义
	ThreadPool(const ThreadPool&) = delete;
	ThreadPool& operator=(const ThreadPool&) = delete;

	// 禁用移动语义
	ThreadPool(ThreadPool&&) = delete;
	ThreadPool& operator=(ThreadPool&&) = delete;

	/* 无参无返回值任务提交接口 */
	void enqueue(std::function<void()> task) {
		{ // unique_lock作用域
			std::unique_lock<std::mutex> lock(queue_mutex_);

			if (stop_) { throw std::runtime_error("此线程池已停止！"); }

			tasks_.emplace(std::move(task)); // 此处emplace更好，当传入lambda函数时原地构造
		}
		cv_.notify_one();
	}
};