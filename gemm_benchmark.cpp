#include <iostream>
#include <vector>
#include <chrono>
#include <fstream>
#include <iomanip>
#include <algorithm>
#include "ThreadPool.h"

// 计算矩阵 C = A * B 中 [start_row, end_row) 这一块的连续行
// 采用 i -> k -> j 循环顺序，保证连续内存访问（提升 CPU Cache 命中率）
void gemm_chunk(const std::vector<double>& A, const std::vector<double>& B, std::vector<double>& C,
                std::size_t N, std::size_t start_row, std::size_t end_row) {
    for (std::size_t i = start_row; i < end_row; ++i) {
        for (std::size_t k = 0; k < N; ++k) {
            double temp = A[i * N + k]; // 减少访问次数
            for (std::size_t j = 0; j < N; ++j) {
                C[i * N + j] += temp * B[k * N + j];
            }
        }
    }
}

// 运行一次指定线程数与分块粒度的压测，返回耗时（毫秒）
double run_parallel_gemm(std::size_t num_threads, std::size_t chunk_size,
                         const std::vector<double>& A, const std::vector<double>& B,
                         std::vector<double>& C, std::size_t N) {
    std::fill(C.begin(), C.end(), 0.0);
    ThreadPool pool(num_threads);
    std::vector<std::future<void>> futures;

    // 计时开始
    const auto start_time = std::chrono::steady_clock::now();


    // 按 chunk_size 步长切分 [0, N) 行，调用 pool.enqueue 投递任务
    for (std::size_t row = 0; row < N; row += chunk_size) {
        std::size_t end_row = std::min(row + chunk_size, N);
        // 在此调用 pool.enqueue 传入 Lambda，内部调用 gemm_chunk(A, B, C, N, row, end_row)
        futures.emplace_back(pool.enqueue([&A, &B, &C, N, row, end_row]() {
            gemm_chunk(A, B, C, N, row, end_row);
            }));
    }

    // 阻塞等待所有分块任务计算完毕
    for (auto& fut : futures) {
        fut.get();
    }

    // TODO 2: 记录结束时间 end_time，计算并返回 double 类型的毫秒耗时
    const auto end_time = std::chrono::steady_clock::now();
    return std::chrono::duration<double, std::milli>(end_time - start_time).count();
}

int main() {
    const std::size_t N = 1024; // 1024 x 1024 矩阵（约 10亿次浮点乘加运算）
    std::vector<double> A(N * N, 1.5);
    std::vector<double> B(N * N, 2.0);
    std::vector<double> C(N * N, 0.0);

    std::cout << "=== 正在运行单线程 Baseline (N = " << N << ") ===\n";
    auto t0 = std::chrono::steady_clock::now();
    gemm_chunk(A, B, C, N, 0, N);
    auto t1 = std::chrono::steady_clock::now();
    double baseline_ms = std::chrono::duration<double, std::milli>(t1 - t0).count();
    std::cout << "Baseline (1 Thread) Time: " << baseline_ms << " ms\n\n";

    std::ofstream csv("benchmark_results_v2.2.csv");
    csv << "experiment,threads,chunk_size,time_ms,speedup\n";

    // 实验 1：不同线程数的扩展性测试（固定每个任务计算 16 行）
    std::cout << "=== 实验 1: 线程扩展性测试 (Chunk Size = 16) ===\n";
    std::vector<std::size_t> thread_counts = { 1, 2, 4, 8, 12, 16, 24, 32 };
    for (std::size_t threads : thread_counts) {
        double ms = run_parallel_gemm(threads, 16, A, B, C, N);
        double speedup = baseline_ms / ms;
        std::cout << "Threads: " << std::setw(2) << threads
            << " | Time: " << std::fixed << std::setprecision(2) << std::setw(7) << ms << " ms"
            << " | Speedup: " << speedup << "x\n";
        csv << "scaling," << threads << ",16," << ms << "," << speedup << "\n";
    }

    // 实验 2：不同任务切分粒度的锁竞争测试（固定 8 线程）
    std::cout << "\n=== 实验 2: 任务粒度与调度开销测试 (Threads = 8) ===\n";
    std::vector<std::size_t> chunk_sizes = { 1, 2, 4, 8, 16, 32, 64, 128 };
    for (std::size_t chunk : chunk_sizes) {
        double ms = run_parallel_gemm(8, chunk, A, B, C, N);
        double speedup = baseline_ms / ms;
        std::cout << "Chunk Size: " << std::setw(3) << chunk
            << " | Time: " << std::fixed << std::setprecision(2) << std::setw(7) << ms << " ms"
            << " | Speedup: " << speedup << "x\n";
        csv << "granularity,8," << chunk << "," << ms << "," << speedup << "\n";
    }

    csv.close();
    std::cout << "\n压测完成！数据已写入 benchmark_results_v2.2.csv\n";
    return 0;
}