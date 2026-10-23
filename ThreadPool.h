#pragma once

#include <vector>
#include <queue>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <functional>
#include <cstddef>
#include <stdexcept>
#include <future>
#include <memory>
#include <type_traits>
#include <concepts>
#include <utility>

class ThreadPool {
private:
	// *类成员对象的初始化严格按照其在类定义中的声明顺序进行，而析构则严格按照声明顺序的逆序进行。*
	/* 状态与同步基础设施 */
	bool stop_{ false };
	std::mutex queue_mutex_;
	std::condition_variable cv_; // jthread中的stoptoken中断信号无法直接被cv识别，需维护stop变量

	/* 被保护的共享资源 */
	std::queue<std::function<void()>> tasks_;

	/* 执行实体 */
	std::vector<std::jthread> workers_;

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
	}

	// 禁用拷贝语义
	ThreadPool(const ThreadPool&) = delete;
	ThreadPool& operator=(const ThreadPool&) = delete;

	// 禁用移动语义
	ThreadPool(ThreadPool&&) = delete;
	ThreadPool& operator=(ThreadPool&&) = delete;

	template<class F,class... Args> requires std::invocable<F, Args...>
	auto enqueue(F&& f, Args&&... args)
	    ->std::future<std::invoke_result_t<F, Args...>> { // 将函数返回值的类型提前推导出来
		using return_type = std::invoke_result_t<F, Args...>;

		auto task = std::make_shared<std::packaged_task<return_type()>>( // packaged_task不可拷贝，利用makeshared造出新对象并拿到指针
			[f = std::forward<F>(f), ...args = std::forward<Args>(args)]() mutable -> return_type
			{return std::invoke(std::move(f), std::move(args)...);}// invoke可以自动推导如成员函数指针传入
		);

		std::future<return_type> res = task->get_future();
		{ // unique_lock作用域
			std::unique_lock<std::mutex> lock(queue_mutex_);

			if (stop_) { throw std::runtime_error("此线程池已停止！"); }

			tasks_.emplace([task]() { // 此处emplace更好，当传入lambda函数时原地构造
				(*task)(); // 将打包好的函数放进无参传入的lambda函数中放进任务队列
				});
		}
		cv_.notify_one();
		return res;
	}
};