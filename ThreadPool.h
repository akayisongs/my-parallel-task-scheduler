#pragma once

#include <vector>
#include <deque>
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
#include <atomic>
#include <algorithm>

namespace szz{
	// 调整类型擦除接口可以放入不可复制移动的函数对象，避免了std::function的拷贝语义限制
	using TaskType = std::move_only_function<void()>;
	using Lock = std::unique_lock<std::mutex>;


	class ThreadPool {
	private:
		// *类成员对象的初始化严格按照其在类定义中的声明顺序进行，而析构则严格按照声明顺序的逆序进行。*
		// 状态与同步基础设施
		bool stop_{ false };
		std::size_t active_tasks_{ 0 };
		mutable std::mutex queue_mutex_; // 允许在const成员函数中加锁读取状态
		std::condition_variable cv_; // jthread中的stoptoken中断信号无法直接被cv识别，需维护stop变量
		std::condition_variable cv_finished_; // 当所有任务全部完成时，唤醒 wait_all()
		std::size_t park_count_{ 0 }; // 累计触发 cv_.wait 陷入内核休眠的总次数
		std::size_t max_depth_{ 0 }; // 任务队列 tasks_ 在运行期间达到的历史最大深度

		// 被保护的共享资源
		std::deque<TaskType> tasks_;

		// 执行实体
		std::vector<std::jthread> workers_;

	public:
		explicit ThreadPool(std::size_t threads) {
			if (threads == 0) {
				throw std::invalid_argument("ThreadPool requires at least 1 worker thread.");
			}

			workers_.reserve(threads);

			try {
				for (std::size_t i = 0;i < threads;i++) {
					workers_.emplace_back([this]() {
						bool has_finished_task = false;
						while (1) {
							TaskType task;// 实际工作任务接口

							{ // unique_lock作用域
								Lock lock(queue_mutex_);

								if (has_finished_task) {
									--active_tasks_;
									has_finished_task = false;
									if (tasks_.empty() && active_tasks_ == 0) { cv_finished_.notify_all(); } // 提醒外部线程（主线程）已完成任务，配合外部waitall使用
								}
								while (!stop_ && tasks_.empty()) {// 此处逻辑为当线程池未停止且队列为空返回0（为0则wait）
									++park_count_; // 统计真正的内核挂起次数
									cv_.wait(lock);
								}
								if (stop_ && tasks_.empty()) { return; }

								task = std::move(tasks_.front());
								tasks_.pop_front();
								++active_tasks_;
							}
							try { task(); }
							catch (...) {
								// 吞掉 enqueue_detached 逃逸的未捕获异常，防止 std::terminate()
							}
							has_finished_task = true;
						}
						});
				}
			}
			catch (...) {
				{
					Lock lock(queue_mutex_);
					stop_ = true;
				}
				cv_.notify_all();
				throw;
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
		template<class F, class... Args> requires std::invocable<F, Args...>
		auto enqueue(F&& f, Args&&... args)
			-> std::future<std::invoke_result_t<F, Args...>> { // 将函数返回值的类型提前推导出来
			using return_type = std::invoke_result_t<F, Args...>;

			auto task = std::packaged_task<return_type()>( // 现在可以直接使用packaged_task而不需要make_shared包装了，由于类型擦除接口TaskType已经可以直接存储不可拷贝移动的函数对象了
				[f = std::forward<F>(f), ...args = std::forward<Args>(args)]() mutable -> return_type
				{return std::invoke(std::move(f), std::move(args)...);}// invoke可以自动推导如成员函数指针传入
			);

			std::future<return_type> res = task.get_future();
			{ // unique_lock作用域
				Lock lock(queue_mutex_);

				if (stop_) { throw std::runtime_error("此线程池已停止！"); }

				tasks_.emplace_back(std::move(task)); // 此处emplace更好，当传入lambda函数时原地构造

				max_depth_ = std::max(max_depth_, tasks_.size());
			}
			cv_.notify_one();
			return res;
		}

		// 入队通道二
		template<class F, class... Args> requires std::invocable<F, Args...>
		void enqueue_detached(F&& f, Args&&... args) { // 无返回值通道，不需要packaged_task包装以及mk_shared做内存分配
			{
				Lock lock(queue_mutex_);

				if (stop_) { throw std::runtime_error("此线程池已停止！"); }

				if constexpr (sizeof...(Args) == 0) {
					tasks_.emplace_back(std::forward<F>(f));
				}
				else {
					tasks_.emplace_back([f = std::forward<F>(f), ...args = std::forward<Args>(args)]() mutable {
						std::invoke(std::move(f), std::move(args)...);
						});
				}
				max_depth_ = std::max(max_depth_, tasks_.size());
			}
			cv_.notify_one();
		}

		// 阻塞等待排队、运行中的全部任务完成
		void wait_all() {
			{
				Lock lock(queue_mutex_);

				cv_finished_.wait(lock, [this]() {return tasks_.empty() && active_tasks_ == 0;});
			}
		}

		// 主动停机
		void shutdown() noexcept {
			std::vector<std::jthread> local_workers;
			{ // unique_lock作用域
				Lock lock(queue_mutex_);
				if (stop_) { return; } // 幂等保护,如果已经停止则直接返回
				stop_ = true;
				local_workers.swap(workers_);
			}
			cv_.notify_all();
			for (auto& worker : local_workers) {
				if (worker.joinable()) {
					if (worker.get_id() == std::this_thread::get_id()) {
						worker.detach();
					}
					else {
						worker.join();
					}
				}
			}
		}

		// 状态查询
		bool is_stopped() const noexcept {
			Lock lock(queue_mutex_);
			return stop_;
		}


		std::size_t pending_tasks() const noexcept {
			Lock lock(queue_mutex_);
			return tasks_.size();
		}

		std::size_t park_count() const noexcept {
			Lock lock(queue_mutex_);
			return park_count_;
		}

		std::size_t max_queue_depth() const noexcept {
			Lock lock(queue_mutex_);
			return max_depth_;
		}
	};
} // namespace szz