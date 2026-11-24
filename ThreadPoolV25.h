#pragma once

#include <vector>
#include <deque>
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


namespace szz::batch {

	// 调整类型擦除接口可以放入不可复制移动的函数对象，避免了std::function的拷贝语义限制
	using TaskType = std::move_only_function<void()>;
	using Lock = std::unique_lock<std::mutex>;

	//// v3.0代码内容
	//struct alignas(64) WorkStealingQueue { // 64字节对齐，避免伪共享
	//	std::deque<TaskType> deque_;
	//	mutable std::mutex mtx_; // 一个队列配一个锁，消除锁竞争，以下互斥锁使用均未大面积涉及系统级调用，开销较小

	//	// 阻塞式入栈
	//	template<class F>
	//	void push(F&& task) {
	//		Lock lock(mtx_);
	//		deque_.emplace_back(std::forward<F>(task));
	//	}

	//	// 非阻塞式入栈
	//	template<class F>
	//	bool try_push(F&& task) {
	//		Lock lock(mtx_, std::try_to_lock);
	//		if (!lock) { return false; }
	//		deque_.emplace_back(std::forward<F>(task));
	//		return true;
	//	}

	//	// 本地线程任务出栈
	//	bool pop(TaskType& task) {
	//		Lock lock(mtx_);
	//		if (deque_.empty()) { return false; }
	//		task = std::move(deque_.back());
	//		deque_.pop_back();
	//		return true;
	//	}

	//	// 从栈底非阻塞窃取任务
	//	bool try_steal(TaskType& task) {
	//		Lock lock(mtx_,std::try_to_lock);
	//		if (!lock.owns_lock() || deque_.empty()) { return false; }
	//		task = std::move(deque_.front());
	//		deque_.pop_front();
	//		return true;
	//	}

	//	bool empty() const noexcept {
	//		Lock lock(mtx_);
	//		return deque_.empty();
	//	}

	//	std::size_t size() const noexcept {
	//		Lock lock(mtx_);
	//		return deque_.size();
	//	}
	//};

	class ThreadPool {
	private:
		// *类成员对象的初始化严格按照其在类定义中的声明顺序进行，而析构则严格按照声明顺序的逆序进行。*
		// 状态与同步基础设施
		bool stop_{ false };
		std::size_t active_tasks_{ 0 };
		mutable std::mutex queue_mutex_; // 允许在const成员函数中加锁读取状态
		std::condition_variable cv_; // jthread中的stoptoken中断信号无法直接被cv识别，需维护stop变量
		std::condition_variable cv_finished_; // 当所有任务全部完成时，唤醒 wait_all()
		std::size_t park_count_{ 0 }; // 累计进入 cv_.wait 等待循环的次数（内核挂起次数的上界）
		std::size_t max_depth_{ 0 }; // 任务队列 tasks_ 在运行期间达到的历史最大深度
		std::size_t consumer_acq_{ 0 };     // 消费者成功批量取任务的次数
		std::size_t tasks_taken_{ 0 };      // 消费者累计取走的任务总数
		std::size_t steady_max_depth_{ 0 }; // 跳过启动瞬态（前 26214 个任务）后的稳态最大队列深度

		// 被保护的共享资源
		std::deque<TaskType> tasks_;

		// 执行实体
		std::vector<std::jthread> workers_;

	public:
		static constexpr std::size_t kConsumerBatchLimit = 64;
		static constexpr std::size_t kProducerBatchLimit = 64;

		explicit ThreadPool(std::size_t threads) {
			if (threads == 0) {
				throw std::invalid_argument("线程池至少需要一个工作线程！");
			}

			workers_.reserve(threads);

			try {
				for (std::size_t i = 0; i < threads; i++) {
					workers_.emplace_back([this, threads]() {

						std::vector<TaskType> local_batch;
						local_batch.reserve(kConsumerBatchLimit);
						std::size_t finished_count = 0;

						while (1) {
							{ // unique_lock作用域
								Lock lock(queue_mutex_);

								if (finished_count > 0) {
									active_tasks_ -= finished_count;
									finished_count = 0;
									if (tasks_.empty() && active_tasks_ == 0) { cv_finished_.notify_all(); } // 提醒外部线程（主线程）已完成任务，配合外部waitall使用
								}

								while (!stop_ && tasks_.empty()) {                                           // 此处逻辑为当线程池未停止且队列为空返回0（为0则wait）
									++park_count_;                                                           // 统计真正的内核挂起次数
									cv_.wait(lock);                                                          // 这里相当于自写逻辑的条件wait
								}

								if ( stop_ && tasks_.empty() ) { return; }

								// 贪心整批出队：队列有多少拿多少，上限为 kConsumerBatchLimit (64)
								const std::size_t take = std::min(kConsumerBatchLimit, tasks_.size());

								for (std::size_t k = 0; k < take; ++k) {
									local_batch.emplace_back(std::move(tasks_.front()));
									tasks_.pop_front();
								}
								active_tasks_ += take;
								++consumer_acq_;
								tasks_taken_ += take;
							}
							for (auto& task : local_batch) {
								try { task(); } catch (...) {
								}
							}
							finished_count = local_batch.size();
							local_batch.clear();
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

		class Batch {
		private:
			ThreadPool& pool_;
			std::size_t limit_;
			std::vector<TaskType> buf_;

		public:
			explicit Batch(ThreadPool& pool, std::size_t limit = kProducerBatchLimit)
				:pool_(pool), limit_(limit) {
				buf_.reserve(limit_);
			}

			~Batch() noexcept {
				try { flush(); } // 析构前将还有未达到limit的任务一并提交
				catch (...) {};
			}

			Batch(const Batch&) = delete;
			Batch& operator=(const Batch&) = delete;
			Batch(Batch&&) = delete;
			Batch& operator=(Batch&&) = delete;

			template<class F, class ...Args>
				requires std::invocable<F, Args...>
			void submit(F&& f, Args&&... args) {
				if constexpr (sizeof...(Args) == 0) { buf_.emplace_back(std::forward<F>(f)); }
				else {
					buf_.emplace_back([f = std::forward<F>(f), ...args = std::forward<Args>(args)]() mutable {
						std::invoke(std::move(f), std::move(args)...);
						});
				}

				if (buf_.size() >= limit_) flush(); // 触发提交
			}

			void flush() {
				if (!buf_.empty()) { pool_.enqueue_bulk(buf_); }
			}
		};

		Batch make_batch(std::size_t limit = kProducerBatchLimit) {
			return Batch(*this, limit);
		}

		// 入队通道一
		template<class F, class... Args>
			requires std::invocable<F, Args...>
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
		template<class F, class... Args>
			requires std::invocable<F, Args...>
		void enqueue_detached(F&& f, Args&&... args) { // 无返回值通道，不需要packaged_task包装以及mk_shared做内存分配
			{
				Lock lock(queue_mutex_);

				if (stop_) { throw std::runtime_error("此线程池已停止！"); }

				if constexpr (sizeof...(Args) == 0) { tasks_.emplace_back(std::forward<F>(f)); }
				else {
					tasks_.emplace_back([f = std::forward<F>(f), ...args = std::forward<Args>(args)]() mutable {
						std::invoke(std::move(f), std::move(args)...);
						});
				}
				max_depth_ = std::max(max_depth_, tasks_.size());
			}
			cv_.notify_one();
		}

		// 批量入队通道
		void enqueue_bulk(std::vector<TaskType>& batch_tasks) {
			if (batch_tasks.empty()) return;
			std::size_t pushed = 0;

			try {
				Lock lock(queue_mutex_);
				if (stop_) {
					batch_tasks.clear(); // 池已停止，清空缓冲防止 ~Batch() 析构时二次重入
					throw std::runtime_error("此线程池已停止！");
				}

				for (auto& v : batch_tasks) {
					tasks_.emplace_back(std::move(v));
					++pushed;
				}
				max_depth_ = std::max(max_depth_, tasks_.size());
				if (tasks_taken_ > 26214) {
					steady_max_depth_ = std::max(steady_max_depth_, tasks_.size());
				}
			}
			catch (...) {
				batch_tasks.clear(); // 避免残留已被 move 掏空的 TaskType 触发 ~Batch() 二次提交
				if (pushed == 1)     cv_.notify_one();
				else if (pushed > 1) cv_.notify_all();
				throw; // 继续向外传递异常
			}

			batch_tasks.clear();

			if (pushed > 0) {
				const std::size_t needed_workers = (pushed + kConsumerBatchLimit - 1) / kConsumerBatchLimit;
				if (needed_workers >= workers_.size()) {
					cv_.notify_all();
				}
				else {
					for (std::size_t i = 0; i < needed_workers; ++i) {
						cv_.notify_one();
					}
				}
			}
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

		std::size_t consumer_acquisitions() const noexcept {
			Lock lock(queue_mutex_);
			return consumer_acq_;
		}

		double avg_take() const noexcept {
			Lock lock(queue_mutex_);
			return consumer_acq_ > 0
				? static_cast<double>(tasks_taken_) / static_cast<double>(consumer_acq_)
				: 0.0;
		}

		std::size_t steady_max_depth() const noexcept {
			Lock lock(queue_mutex_);
			return steady_max_depth_;
		}
	};
} // namespace szz::batch
