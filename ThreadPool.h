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
	std::size_t active_tasks_{ 0 };
	mutable std::mutex queue_mutex_; // 允许在const成员函数中加锁读取状态
	std::condition_variable cv_; // jthread中的stoptoken中断信号无法直接被cv识别，需维护stop变量
	std::condition_variable cv_finished_; // 当所有任务全部完成时，唤醒 wait_all()

	/* 被保护的共享资源 */
	std::queue<std::function<void()>> tasks_;

	/* 执行实体 */
	std::vector<std::jthread> workers_;

public:
	explicit ThreadPool(std::size_t threads) {
		workers_.reserve(threads);

		for (std::size_t i = 0;i < threads;i++) {
			workers_.emplace_back([this]() {
				bool has_finished_task = false;
				while (1) {
					std::function<void()> task;// 实际工作任务接口

					{ // unique_lock作用域
						std::unique_lock lock(queue_mutex_);

						if (has_finished_task) {
							--active_tasks_;
							has_finished_task = false;
							if (tasks_.empty() && active_tasks_ == 0) { cv_finished_.notify_all(); } // 提醒外部线程（主线程）已完成任务，配合外部waitall使用
						}

						cv_.wait(lock, [this]() { return stop_ || !tasks_.empty();});// 此处逻辑为当线程池未停止且队列为空返回0（为0则wait）

						if (stop_ && tasks_.empty()) { return; }

						task = std::move(tasks_.front());
						tasks_.pop();
						++active_tasks_;
					}
					task();
					has_finished_task = true;
				}
				});
		}
	}
	~ThreadPool() noexcept {
		shutdown();
	}

	// 禁用拷贝语义
	ThreadPool(const ThreadPool&) = delete;
	ThreadPool& operator=(const ThreadPool&) = delete;

	// 禁用移动语义
	ThreadPool(ThreadPool&&) = delete;
	ThreadPool& operator=(ThreadPool&&) = delete;

	// 入队通道一
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

	// 入队通道二
	template<class F,class... Args> requires std::invocable<F,Args...>
	void enqueue_detached(F&& f, Args&&... args) { // 无返回值通道，不需要packaged_task包装以及mk_shared做内存分配
		{
			std::unique_lock<std::mutex> lock(queue_mutex_);

			if (stop_) { throw std::runtime_error("此线程池已停止！"); }

			if constexpr (sizeof...(Args) == 0) {
				tasks_.emplace(std::forward<F>(f));
			}
			else {
				tasks_.emplace([f = std::forward<F>(f), ...args = std::forward<Args>(args)]() mutable {
					std::invoke(std::move(f), std::move(args)...);
					});
			}
		}
		cv_.notify_one();
	}

	// 阻塞等待排队、运行中的全部任务完成
	void wait_all() {
		{
			std::unique_lock<std::mutex> lock(queue_mutex_);

			cv_finished_.wait(lock, [this]() {return tasks_.empty() && active_tasks_ == 0;});
		}
	}

	// 主动停机
	void shutdown() noexcept {

		{ // unique_lock作用域
			std::unique_lock<std::mutex> lock(queue_mutex_);
			if (stop_) { return; } // 幂等保护,如果已经停止则直接返回
			stop_ = true;
		}
		cv_.notify_all();
		workers_.clear();
	}

	// 状态查询
	bool is_stopped() const noexcept {
		std::unique_lock<std::mutex> lock(queue_mutex_);
		return stop_;
	}


	std::size_t pending_tasks() const noexcept {
		std::unique_lock<std::mutex> lock(queue_mutex_);
		return tasks_.size();
	}
};