#include <iostream>
#include <vector>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <algorithm>
#include "ThreadPool.h"

// 按一维展平的任务区间 [start_idx, end_idx) 计算矩阵 C 的元素
// 这样可以支持从“整块矩阵”一直切碎到“单个元素（104万个微任务）”！
void gemm_range(const std::vector<double>& A, const std::vector<double>& B, std::vector<double>& C,
    std::size_t N, std::size_t start_idx, std::size_t end_idx) {
    // 如果区间恰好是整行，走高缓存命中率的 i -> k -> j 分支
    if (start_idx % N == 0 && end_idx % N == 0) {
        std::size_t start_row = start_idx / N;
        std::size_t end_row = end_idx / N;
        for (std::size_t i = start_row; i < end_row; ++i) {
            for (std::size_t k = 0; k < N; ++k) {
                double temp = A[i * N + k];
                for (std::size_t j = 0; j < N; ++j) {
                    C[i * N + j] += temp * B[k * N + j];
                }
            }
        }
    }
    else {
        // 细粒度元素级区间计算
        for (std::size_t idx = start_idx; idx < end_idx; ++idx) {
            std::size_t i = idx / N;
            std::size_t j = idx % N;
            double sum = 0.0;
            for (std::size_t k = 0; k < N; ++k) {
                sum += A[i * N + k] * B[k * N + j];
            }
            C[idx] = sum;
        }
    }
}

// 运行指定线程数与每个任务负责的行数（跑 iterations 次取平均耗时）
double run_parallel_rows(std::size_t num_threads, std::size_t rows_per_task,
    const std::vector<double>& A, const std::vector<double>& B,
    std::vector<double>& C, std::size_t N, int iterations = 3) {
    ThreadPool pool(num_threads); // 线程池复用，消除线程创建销毁的干扰
    double total_ms = 0.0;

    for (int iter = 0; iter < iterations; ++iter) {
        std::fill(C.begin(), C.end(), 0.0);
        std::vector<std::future<void>> futures;
        futures.reserve((N + rows_per_task - 1) / rows_per_task);

        const auto start_time = std::chrono::steady_clock::now();

        for (std::size_t row = 0; row < N; row += rows_per_task) {
            std::size_t end_row = std::min(row + rows_per_task, N);
            futures.emplace_back(pool.enqueue([&A, &B, &C, N, row, end_row]() {
                gemm_range(A, B, C, N, row * N, end_row * N);
                }));
        }

        for (auto& fut : futures) {
            fut.get();
        }

        const auto end_time = std::chrono::steady_clock::now();
        total_ms += std::chrono::duration<double, std::milli>(end_time - start_time).count();
    }
    return total_ms / iterations;
}

// 固定总工作量，测试拆分为不同数量的任务包（从 64 个包 到 262,144 个微任务包）
double run_granularity_test(std::size_t num_threads, std::size_t num_tasks,
    const std::vector<double>& A, const std::vector<double>& B,
    std::vector<double>& C, std::size_t N) {
    ThreadPool pool(num_threads);
    std::fill(C.begin(), C.end(), 0.0);

    std::size_t total_elements = N * N;
    std::size_t elements_per_task = (total_elements + num_tasks - 1) / num_tasks;

    const auto start_time = std::chrono::steady_clock::now();

    for (std::size_t idx = 0; idx < total_elements; idx += elements_per_task) {
        std::size_t end_idx = std::min(idx + elements_per_task, total_elements);
        pool.enqueue_detached([&A, &B, &C, idx, end_idx]() {
            // 为了纯粹测量“调度器锁竞争与任务分发开销”，所有粒度统一执行恒定计算量
            for (std::size_t pos = idx; pos < end_idx; ++pos) {
                C[pos] = A[pos] * B[pos] + 1.0;
            }
            });
    }

    pool.wait_all();

    const auto end_time = std::chrono::steady_clock::now();
    return std::chrono::duration<double, std::milli>(end_time - start_time).count();
}

int main() {
    const std::size_t N = 1024;
    std::vector<double> A(N * N, 1.5);
    std::vector<double> B(N * N, 2.0);
    std::vector<double> C(N * N, 0.0);

    // CPU 预热（Warm-up），拉满睿频与操作系统内存页映射
    std::cout << "=== 正在进行 CPU 与 Cache 预热 (Warm-up)... ===\n";
    gemm_range(A, B, C, N, 0, N * N);

    // 单线程 Baseline 连跑 3 次取平均值
    std::cout << "=== 正在测量单线程 Baseline (N = " << N << ", 3 次取平均) ===\n";
    double baseline_total = 0.0;
    for (int i = 0; i < 3; ++i) {
        std::fill(C.begin(), C.end(), 0.0);
        auto t0 = std::chrono::steady_clock::now();
        gemm_range(A, B, C, N, 0, N * N);
        auto t1 = std::chrono::steady_clock::now();
        baseline_total += std::chrono::duration<double, std::milli>(t1 - t0).count();
    }
    double baseline_ms = baseline_total / 3.0;
    std::cout << "Baseline (1 Thread) Avg Time: " << std::fixed << std::setprecision(2) << baseline_ms << " ms\n\n";

    std::ofstream csv("benchmark_results_v2.4.csv");
    csv << "experiment,param,time_ms,speedup\n";

    // 不同线程数的扩展性测试（固定每次切 16 行，跑 3 次取平均）
    std::cout << "=== 实验 1: 线程扩展性测试 (Chunk = 16 Rows, 3 次平均) ===\n";
    std::vector<std::size_t> thread_counts = { 1, 2, 4, 8, 12, 16, 24, 32 };
    for (std::size_t threads : thread_counts) {
        double ms = run_parallel_rows(threads, 16, A, B, C, N, 3);
        double speedup = baseline_ms / ms;
        std::cout << "Threads: " << std::setw(2) << threads
            << " | Avg Time: " << std::setw(7) << ms << " ms"
            << " | Speedup: " << speedup << "x\n";
        csv << "scaling," << threads << "," << ms << "," << speedup << "\n";
    }

    // 不同任务切分数量的锁竞争压测（固定 8 线程，将 104万次操作拆分为 16 ~ 262,144 个任务包）
    std::cout << "\n=== 实验 2: 任务切分粒度与全局锁竞争压测 (Threads = 8) ===\n";
    std::vector<std::size_t> task_counts = { 16, 64, 256, 1024, 4096, 16384, 65536, 262144 };
    for (std::size_t tasks : task_counts) {
        double ms = run_granularity_test(8, tasks, A, B, C, N);
        std::cout << "Total Tasks: " << std::setw(6) << tasks
            << " | Time: " << std::setw(7) << ms << " ms\n";
        csv << "granularity," << tasks << "," << ms << ",0\n";
    }

    csv.close();
    std::cout << "\n压测完成！修正后的数据已写入 benchmark_results_v2.4.csv\n";
    return 0;
}