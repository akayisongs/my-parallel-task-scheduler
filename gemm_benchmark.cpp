#ifdef _WIN32
#ifndef NOMINMAX
#define NOMINMAX
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#endif

#include <algorithm>
#include <charconv>
#include <chrono>
#include <cmath>
#include <concepts>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>
#include "SchedulerContract.h"
#include "ThreadPool.h"

#if __has_include("ThreadPoolV25.h")
#if defined(__clang__)
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wunused-lambda-capture"
#endif
#include "ThreadPoolV25.h"
#if defined(__clang__)
#pragma clang diagnostic pop
#endif
#define BENCH_HAS_V25 1
#else
#define BENCH_HAS_V25 0
#endif

static_assert(szz::contract::TaskScheduler<szz::ThreadPool>);
static_assert(szz::contract::DetachedSubmittable<szz::ThreadPool, void (*)()>);
static_assert(szz::contract::FutureSubmittable<szz::ThreadPool, void (*)()>);
static_assert(szz::contract::HasStats<szz::ThreadPool>);

#if BENCH_HAS_V25
static_assert(szz::contract::TaskScheduler<szz::batch::ThreadPool>);
static_assert(szz::contract::DetachedSubmittable<szz::batch::ThreadPool, void (*)()>);
static_assert(szz::contract::FutureSubmittable<szz::batch::ThreadPool, void (*)()>);
static_assert(szz::contract::HasStats<szz::batch::ThreadPool>);
static_assert(szz::contract::BatchSubmittable<szz::batch::ThreadPool>);
static_assert(szz::contract::HasBatchStats<szz::batch::ThreadPool>);
#endif

template <class T>
inline void do_not_optimize(const T& value) noexcept {
#if defined(__GNUC__) || defined(__clang__)
    asm volatile("" : : "r,m"(value) : "memory");
#else
    volatile auto sink = value;
    (void)sink;
#endif
}

inline void verify_array(const std::vector<double>& C, double expected, const char* label) {
    double checksum = 0.0;
    for (std::size_t i = 0; i < C.size(); ++i) {
        if (std::abs(C[i] - expected) > 1e-6) {
            std::cerr << "\n[VALIDATION ERROR] " << label << " mismatch at index " << i
                      << ": expected " << expected << ", got " << C[i] << '\n';
            std::exit(1);
        }
        checksum += C[i];
    }
    do_not_optimize(checksum);
}

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
    szz::ThreadPool pool(num_threads); // 线程池复用，消除线程创建销毁的干扰
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
        verify_array(C, 3.0 * static_cast<double>(N), "run_parallel_rows");
    }
    return total_ms / iterations;
}

// 固定总工作量，测试拆分为不同数量的任务包（从 64 个包 到 262,144 个微任务包）
double run_granularity_test(std::size_t num_threads, std::size_t num_tasks,
    const std::vector<double>& A, const std::vector<double>& B,
    std::vector<double>& C, std::size_t N) {
    szz::ThreadPool pool(num_threads);
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
    verify_array(C, 4.0, "run_granularity_test");
    return std::chrono::duration<double, std::milli>(end_time - start_time).count();
}

struct GemmCliOptions {
    bool help{ false };
    bool profile{ false };
    std::string arm{ "v2.4" };
    std::size_t threads{ 8 };
    std::size_t rows{ 16 };
    std::size_t size{ 1024 };
    std::size_t waves{ 5 };
};

enum class GemmFlagMatch { NoMatch, Matched, Error };

inline bool parse_positive_size(std::string_view sv, std::size_t& out) noexcept {
    if (sv.empty()) return false;
    std::size_t val = 0;
    const char* begin = sv.data();
    const char* end = begin + sv.size();
    const auto [ptr, ec] = std::from_chars(begin, end, val);
    if (ec != std::errc{} || ptr != end || val == 0) return false;
    out = val;
    return true;
}

inline GemmFlagMatch match_flag_value(int argc, char** argv, int& idx,
    std::string_view name, std::string_view& out_val, std::string& err) {
    const std::string_view arg(argv[idx]);
    if (arg == name) {
        if (idx + 1 >= argc) {
            err = "missing value for " + std::string(name);
            return GemmFlagMatch::Error;
        }
        out_val = argv[++idx];
        if (out_val.empty()) {
            err = "empty value for " + std::string(name);
            return GemmFlagMatch::Error;
        }
        return GemmFlagMatch::Matched;
    }
    if (arg.starts_with(name) && arg.size() > name.size() && arg[name.size()] == '=') {
        out_val = arg.substr(name.size() + 1);
        if (out_val.empty()) {
            err = "empty value for " + std::string(name);
            return GemmFlagMatch::Error;
        }
        return GemmFlagMatch::Matched;
    }
    return GemmFlagMatch::NoMatch;
}

inline void print_usage(std::ostream& os, const char* prog) {
    os << "Usage:\n"
       << "  " << prog << "            Run legacy baseline + scaling + granularity suite (writes CSV)\n"
       << "  " << prog << " --profile [--arm <v2.4|v2.5>] [--threads <T>]"
       << " [--rows <R>] [--size <N>] [--waves <W>]\n"
       << "  " << prog << " --help\n\n"
       << "Profile options (no warmup, no baseline, no experiment 2, no CSV):\n"
       << "  --help         Show this usage and exit 0\n"
       << "  --profile      Enable single-arm single-point perf isolation mode\n"
       << "  --arm <name>   Scheduler arm: v2.4"
#if BENCH_HAS_V25
       << " or v2.5"
#endif
       << " (default: v2.4)\n"
       << "  --threads <T>  Worker thread count >= 1 (default: 8)\n"
       << "  --rows <R>     Rows per task >= 1 (default: 16)\n"
       << "  --size <N>     Matrix dimension N >= 1 (default: 1024)\n"
       << "  --waves <W>    Measured waves >= 1 (default: 5)\n";
}

inline bool parse_cli_args(int argc, char** argv, GemmCliOptions& opts, std::string& err) {
    for (int i = 1; i < argc; ++i) {
        if (std::string_view(argv[i]) == "--help") {
            opts.help = true;
            return true;
        }
    }

    struct SizeFlag {
        std::string_view name;
        std::size_t* target;
    };
    const SizeFlag size_flags[] = {
        { "--threads", &opts.threads },
        { "--rows",    &opts.rows },
        { "--size",    &opts.size },
        { "--waves",   &opts.waves },
    };

    for (int i = 1; i < argc; ++i) {
        const std::string_view arg(argv[i]);
        if (arg == "--profile") {
            opts.profile = true;
            continue;
        }

        std::string_view val;
        const GemmFlagMatch arm_match = match_flag_value(argc, argv, i, "--arm", val, err);
        if (arm_match == GemmFlagMatch::Error) return false;
        if (arm_match == GemmFlagMatch::Matched) {
            opts.arm = std::string(val);
            continue;
        }

        bool matched_size = false;
        for (const auto& sf : size_flags) {
            const GemmFlagMatch m = match_flag_value(argc, argv, i, sf.name, val, err);
            if (m == GemmFlagMatch::Error) return false;
            if (m == GemmFlagMatch::Matched) {
                if (!parse_positive_size(val, *sf.target)) {
                    err = "invalid positive integer for " + std::string(sf.name) + ": " + std::string(val);
                    return false;
                }
                matched_size = true;
                break;
            }
        }
        if (matched_size) continue;

        err = "unknown or unexpected argument: " + std::string(arg);
        return false;
    }

    if (!opts.profile) {
        err = "--profile is required when arguments are provided";
        return false;
    }
    if (opts.arm == "v2.5") {
#if !BENCH_HAS_V25
        err = "--arm v2.5 is unavailable (ThreadPoolV25.h not found)";
        return false;
#endif
    }
    else if (opts.arm != "v2.4") {
        err = "unknown --arm '" + opts.arm + "' (expected v2.4"
#if BENCH_HAS_V25
            " or v2.5"
#endif
            ")";
        return false;
    }
    return true;
}

template <szz::contract::TaskScheduler Pool>
void run_profile_gemm(const char* arm_name, const GemmCliOptions& opts) {
    const std::size_t N = opts.size;
    const std::size_t rows_per_task = opts.rows;
    const std::size_t n_tasks = (N + rows_per_task - 1) / rows_per_task;

    std::vector<double> A(N * N, 1.5);
    std::vector<double> B(N * N, 2.0);
    std::vector<double> C(N * N, 0.0);

    Pool pool(opts.threads);
    double total_ms = 0.0;

    for (std::size_t w = 0; w < opts.waves; ++w) {
        // gemm_range 整行分支使用 += 累加写入，每轮运行前必须清零 C
        std::fill(C.begin(), C.end(), 0.0);

        const auto t0 = std::chrono::steady_clock::now();
        if constexpr (szz::contract::BatchSubmittable<Pool>) {
            {
                auto batch = pool.make_batch();
                for (std::size_t row = 0; row < N; row += rows_per_task) {
                    const std::size_t end_row = std::min(row + rows_per_task, N);
                    batch.submit([&A, &B, &C, N, row, end_row]() {
                        gemm_range(A, B, C, N, row * N, end_row * N);
                    });
                }
                batch.flush();
            }
        }
        else {
            for (std::size_t row = 0; row < N; row += rows_per_task) {
                const std::size_t end_row = std::min(row + rows_per_task, N);
                pool.enqueue_detached([&A, &B, &C, N, row, end_row]() {
                    gemm_range(A, B, C, N, row * N, end_row * N);
                });
            }
        }
        pool.wait_all();
        const auto t1 = std::chrono::steady_clock::now();

        total_ms += std::chrono::duration<double, std::milli>(t1 - t0).count();

        if (w == 0 || w + 1 == opts.waves) {
            verify_array(C, 3.0 * static_cast<double>(N), "profile_gemm");
        }
    }

    const double avg_ms = total_ms / static_cast<double>(opts.waves);
    std::cout << "[profile] arm=" << arm_name
              << " threads=" << opts.threads
              << " size=" << N
              << " rows=" << rows_per_task
              << " n_tasks=" << n_tasks
              << " waves=" << opts.waves
              << std::fixed << std::setprecision(3)
              << " total_ms=" << total_ms
              << " avg_ms=" << avg_ms;

    if constexpr (szz::contract::HasStats<Pool>) {
        std::cout << " parks=" << pool.park_count()
                  << " depth=" << pool.max_queue_depth();
    }
    if constexpr (szz::contract::HasBatchStats<Pool>) {
        std::cout << " acq=" << pool.consumer_acquisitions()
                  << " avg_take=" << std::setprecision(1) << pool.avg_take()
                  << " s_depth=" << pool.steady_max_depth();
    }
    std::cout << '\n';
}

int main(int argc, char** argv) {
#ifdef _WIN32
    SetConsoleOutputCP(CP_UTF8);
#endif
    if (argc > 1) {
        GemmCliOptions opts;
        std::string err;
        if (!parse_cli_args(argc, argv, opts, err)) {
            std::cerr << "Error: " << err << "\n\n";
            print_usage(std::cerr, argv[0]);
            return 1;
        }
        if (opts.help) {
            print_usage(std::cout, argv[0]);
            return 0;
        }
        if (opts.arm == "v2.4") {
            run_profile_gemm<szz::ThreadPool>("v2.4", opts);
        }
#if BENCH_HAS_V25
        else if (opts.arm == "v2.5") {
            run_profile_gemm<szz::batch::ThreadPool>("v2.5", opts);
        }
#endif
        return 0;
    }

    const std::size_t N = 1024;
    std::vector<double> A(N * N, 1.5);
    std::vector<double> B(N * N, 2.0);
    std::vector<double> C(N * N, 0.0);

    // CPU 预热（Warm-up），拉满睿频与操作系统内存页映射
    std::cout << "=== 正在进行 CPU 与 Cache 预热 (Warm-up)... ===\n";
    gemm_range(A, B, C, N, 0, N * N);
    verify_array(C, 3.0 * static_cast<double>(N), "warmup_gemm");

    // 单线程 Baseline 连跑 3 次取平均值
    std::cout << "=== 正在测量单线程 Baseline (N = " << N << ", 3 次取平均) ===\n";
    double baseline_total = 0.0;
    for (int i = 0; i < 3; ++i) {
        std::fill(C.begin(), C.end(), 0.0);
        auto t0 = std::chrono::steady_clock::now();
        gemm_range(A, B, C, N, 0, N * N);
        auto t1 = std::chrono::steady_clock::now();
        baseline_total += std::chrono::duration<double, std::milli>(t1 - t0).count();
        verify_array(C, 3.0 * static_cast<double>(N), "baseline_gemm");
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