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
#include <algorithm>

namespace szz::v26 {

    using TaskType = std::move_only_function<void()>;
    using Lock = std::unique_lock<std::mutex>;
    using size_t = std::size_t;

    class ThreadPool {
    private:
        bool stop_{ false };
        size_t active_tasks_{ 0 };

        mutable std::mutex queue_mutex_;
        std::condition_variable cv_;
        std::condition_variable cv_finished_;

        // 诊断探针（注意：所有深度探针的计量单位仍须保持为“任务个数”，以便与 v2.4/v2.5 横向可比）
        size_t park_count_{ 0 };
        size_t max_depth_{ 0 };
        size_t consumer_acq_{ 0 };
        size_t tasks_taken_{ 0 };
        size_t steady_max_depth_{ 0 };
        size_t pending_tasks_{ 0 };

        // ====================================================================
        // [TODO 1] 升级底层队列容器与 O(1) 任务计数器
        // --------------------------------------------------------------------
        // 背景：v2.5 使用 std::deque<TaskType>，锁内每次搬运单个任务是 O(B) 复杂度。
        // 需求：
        //   1. 定义新的队列容器 batches_，元素类型为“存满一批 TaskType 的动态数组”。
        //   2. 思考：当队列元素变成批次容器后，batches_.size() 返回的是“批次数”而非“总任务数”。
        //      若在 pending_tasks() 中遍历所有批次求和，复杂度会退化为 O(N)。
        //      请额外定义一个成员变量 pending_tasks_，用于在锁内以 O(1) 复杂度维护当前排队中的总任务数。
        // ====================================================================

        std::deque<std::vector<TaskType>> batches_;


        std::vector<std::jthread> workers_;

        // ====================================================================
        // [TODO 7] 封装队列深度遥测更新辅助函数
        // --------------------------------------------------------------------
        // 前置条件：调用方已持有 queue_mutex_，且已更新 pending_tasks_。
        // 需求：
        //   1. 用当前排队中的“总任务数”（而非批次数）更新 max_depth_。
        //   2. 当累计取走的任务数 tasks_taken_ 超过 26214（跳过前 10% 启动瞬态）时，
        //      同步更新稳态最大深度 steady_max_depth_。
        // ====================================================================
        void update_depth_telemetry() noexcept {
            // 在此编写 [TODO 7] 的实现：

        }

    public:
        static constexpr size_t kConsumerBatchLimit = 64;
        static constexpr size_t kProducerBatchLimit = 64;

        explicit ThreadPool(size_t threads) {
            if (threads == 0) {
                throw std::invalid_argument("线程池至少需要一个工作线程！");
            }

            workers_.reserve(threads);

            try {
                for (size_t i = 0; i < threads; ++i) {
                    workers_.emplace_back([this]() {
                        std::vector<TaskType> local_batch;
                        size_t finished_count = 0;

                        while (true) {
                            {
                                Lock lock(queue_mutex_);

                                // 归还上一批次完成的任务数（锁合并）
                                if (finished_count > 0) {
                                    active_tasks_ -= finished_count;
                                    finished_count = 0;
                                    // 注意：此处的队列判空条件需与 [TODO 1] 的新容器对齐
                                    if (batches_.empty() && active_tasks_ == 0) {
                                        cv_finished_.notify_all();
                                    }
                                }

                                while (!stop_ && batches_.empty()) {
                                    ++park_count_;
                                    cv_.wait(lock);
                                }

                                if (stop_ && batches_.empty()) {
                                    return;
                                }

                                // ============================================================
                                // [TODO 2] 消费端：锁内 O(1) 整批指针接管与状态记账
                                // ------------------------------------------------------------
                                // 目标：消除 v2.5 中的 for 循环逐个移动，在 O(1) 时间内取走队首整批任务。
                                // 步骤：
                                //   1. 将队首批次容器通过移动语义（std::move）直接赋值给 local_batch，
                                //      并将队首元素从 batches_ 中弹出。
                                //   2. 获取本次取走的实际任务数量 take = local_batch.size()。
                                //   3. 同步更新 4 个状态量：
                                //      - pending_tasks_（减去 take）
                                //      - active_tasks_ （加上 take）
                                //      - consumer_acq_ （自增 1 次）
                                //      - tasks_taken_  （加上 take）
                                // ============================================================

                                local_batch = std::move(batches_.front());
                                batches_.pop_front();

                                auto take = local_batch.size();
                                pending_tasks_ -= take;
                                active_tasks_ += take;
                                consumer_acq_++;
                                tasks_taken_ += take;
                            }

                            // 锁外连续执行本批次所有任务
                            for (auto& task : local_batch) {
                                try {
                                    task();
                                }
                                catch (...) {
                                }
                            }
                            finished_count = local_batch.size();

                            // ================================================================
                            // [TODO 3] 消费端：锁外释放旧缓冲区（防锁内隐式 operator delete）
                            // ----------------------------------------------------------------
                            // 思考与查阅（请查阅 cppreference 中 std::vector::operator= 的移动赋值说明）：
                            //   如果这里像 v2.5 一样只写 local_batch.clear()，local_batch 的 capacity()
                            //   依然是 64（持有着 4KB 堆内存）。
                            //   那么下一轮循环进入 Lock lock(queue_mutex_) 执行 [TODO 2] 的
                            //   local_batch = std::move(batches_.front()) 时，vector 的移动赋值运算符
                            //   会先销毁并释放（deallocate）local_batch 原有的 4KB 缓冲！
                            //   这意味着堆内存释放操作又被带回了全局锁的临界区内！
                            //
                            // 需求：
                            //   请在当前锁外位置，将 local_batch 中的元素析构，并使其底层堆内存也在此处释放，
                            //   确保下一轮进入锁内前 local_batch.capacity() == 0。
                            //   （提示：可利用局部作用域临时 vector 移动接管，或查阅 swap / shrink_to_fit）
                            // ================================================================



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

        ThreadPool(const ThreadPool&) = delete;
        ThreadPool& operator=(const ThreadPool&) = delete;
        ThreadPool(ThreadPool&&) = delete;
        ThreadPool& operator=(ThreadPool&&) = delete;

        // 生产端显式 RAII 批量缓冲器（接口与 v2.5 保持一致）
        class Batch {
        private:
            ThreadPool& pool_;
            size_t limit_;
            std::vector<TaskType> buf_;

        public:
            explicit Batch(ThreadPool& pool, size_t limit = kProducerBatchLimit)
                : pool_(pool), limit_(limit) {
                buf_.reserve(limit_);
            }

            ~Batch() noexcept {
                try { flush(); }
                catch (...) {}
            }

            Batch(const Batch&) = delete;
            Batch& operator=(const Batch&) = delete;
            Batch(Batch&&) = delete;
            Batch& operator=(Batch&&) = delete;

            template<class F, class... Args>
                requires std::invocable<F, Args...>
            void submit(F&& f, Args&&... args) {
                if constexpr (sizeof...(Args) == 0) {
                    buf_.emplace_back(std::forward<F>(f));
                }
                else {
                    buf_.emplace_back([f = std::forward<F>(f), ...args = std::forward<Args>(args)]() mutable {
                        std::invoke(std::move(f), std::move(args)...);
                        });
                }

                if (buf_.size() >= limit_) {
                    flush();
                }
            }

            void flush() {
                if (!buf_.empty()) {
                    pool_.enqueue_bulk(buf_);
                }
            }
        };

        Batch make_batch(size_t limit = kProducerBatchLimit) {
            return Batch(*this, limit);
        }

        // ====================================================================
        // [TODO 4] 生产端：enqueue_bulk 锁内 O(1) 整批入队与锁外容量恢复
        // --------------------------------------------------------------------
        // 参数：batch_tasks 是调用方 Batch 对象内部的私有缓冲 vector (buf_)。
        // 思考与查阅：
        //   1. 当你在锁内将整个 batch_tasks 移动（std::move）进 batches_ 队列后，
        //      batch_tasks 变成了 moved-from 状态（其 size() 和 capacity() 均变为 0）。
        //   2. 如果直接返回，调用方接下来继续调用 batch.submit() 塞下一批的 64 个任务时，
        //      buf_ 会从 capacity=0 开始经历 1->2->4->8->16->32->64 共 7 次几何扩容！
        //
        // 实现要求：
        //   1. 边界检查：若 batch_tasks 为空则直接返回；预先记录其元素个数 count 与原有容量 cap。
        //   2. 锁内操作：
        //      - 检查 stop_：若已停止，清空 batch_tasks（防止 ~Batch() 析构时二次重入）并抛异常。
        //      - 将整个 batch_tasks 以 O(1) 移动方式推入 batches_ 队尾。
        //      - 更新 pending_tasks_ 并调用 update_depth_telemetry()。
        //      （思考：为什么整批 vector 移动天生具备强异常安全 guarantee，不再需要 v2.5 的 try-catch 补通知？）
        //   3. 锁外操作：
        //      - 对 batch_tasks 调用 clear() 并重新 reserve(cap)，在锁外一次性恢复其容量。
        //      - 调用 cv_.notify_one() 唤醒 1 个工作线程来取走这整批任务。
        // ====================================================================
        void enqueue_bulk(std::vector<TaskType>& batch_tasks) {
            // 在此编写 [TODO 4] 的实现：

        }

        // ====================================================================
        // [TODO 5] 单任务兼容通道：enqueue 与 enqueue_detached 的“尾批合并”
        // --------------------------------------------------------------------
        // 背景：虽然压测走的是 Batch，但线程池仍需满足 TaskScheduler / DetachedSubmittable 契约。
        // 问题：既然队列类型变成了 std::deque<std::vector<TaskType>>，当外部只提交 1 个单任务时，
        //      如果每次都新建一个只含 1 个元素的 vector 塞进 deque，不仅浪费内存，还会让消费者一次只拿到 1 个任务。
        //
        // 尾批合并策略（Tail-Batch Coalescing）：
        //   在锁内准备放入单个 task 时，检查队列尾部批次：
        //   - 若 batches_ 非空，且队尾批次 batches_.back().size() < kProducerBatchLimit：
        //     直接把该 task 追加入队尾批次 batches_.back() 中！
        //   - 否则（队列为空，或队尾批次已满 64 个）：
        //     新建一个单元素 std::vector<TaskType>，放入 task 后移动进 batches_ 队尾。
        //   最后别忘了更新 pending_tasks_、调用 update_depth_telemetry() 并在锁外 notify_one()。
        // ====================================================================
        template<class F, class... Args>
            requires std::invocable<F, Args...>
        auto enqueue(F&& f, Args&&... args)
            -> std::future<std::invoke_result_t<F, Args...>> {
            using return_type = std::invoke_result_t<F, Args...>;

            auto task = std::packaged_task<return_type()>(
                [f = std::forward<F>(f), ...args = std::forward<Args>(args)]() mutable -> return_type {
                    return std::invoke(std::move(f), std::move(args)...);
                }
            );

            std::future<return_type> res = task.get_future();

            // 在此编写 [TODO 5.1] enqueue 的锁内尾批合并与通知逻辑：


            return res;
        }

        template<class F, class... Args>
            requires std::invocable<F, Args...>
        void enqueue_detached(F&& f, Args&&... args) {
            // 在此编写 [TODO 5.2] enqueue_detached 的任务构造、锁内尾批合并与通知逻辑：

        }

        // ====================================================================
        // [TODO 6] 同步等待与状态查询接口适配
        // --------------------------------------------------------------------
        // 需求：将 wait_all() 和 pending_tasks() 适配到新的 batches_ 和 pending_tasks_ 上。
        // ====================================================================
        void wait_all() {
            // 在此编写 [TODO 6.1] 的实现：

        }

        void shutdown() noexcept {
            std::vector<std::jthread> local_workers;
            {
                Lock lock(queue_mutex_);
                if (stop_) { return; }
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

        bool is_stopped() const noexcept {
            Lock lock(queue_mutex_);
            return stop_;
        }

        size_t pending_tasks() const noexcept {
            // 在此编写 [TODO 6.2] 的实现（要求 O(1) 复杂度）：

        }

        size_t park_count() const noexcept {
            Lock lock(queue_mutex_);
            return park_count_;
        }

        size_t max_queue_depth() const noexcept {
            Lock lock(queue_mutex_);
            return max_depth_;
        }

        size_t consumer_acquisitions() const noexcept {
            Lock lock(queue_mutex_);
            return consumer_acq_;
        }

        double avg_take() const noexcept {
            Lock lock(queue_mutex_);
            return consumer_acq_ > 0
                ? static_cast<double>(tasks_taken_) / static_cast<double>(consumer_acq_)
                : 0.0;
        }

        size_t steady_max_depth() const noexcept {
            Lock lock(queue_mutex_);
            return steady_max_depth_;
        }
    };

} // namespace szz::v26