// ============================================================================
//  bench_scheduler.cpp — 同进程 A/B + 假设验证
//
//  三件事：
//    A. 基线 vs 候选 同进程对比（波次交错，抵消环境漂移）
//    B. 复刻历史实验（线程扩展 / 粒度扫描），保持与旧 CSV 可比
//    C. 验证假设：
//         H1 生产者速率 vs 消费者容量   → 扫生产者数量 k
//         H2 是否每个任务触发一次内核挂起 → park_count
//         H3 队列是否真的从不堆积        → max_queue_depth
//
//  设计约束：
//    * 计时区间 = 派发 + wait_all，不含建池；producer 线程的创建与 join 计入
//    * 每点 N 次采样，报 median + IQR，丢第 0 波
//    * 统计量在计时区外读取，不污染测量
// ============================================================================
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
#include <thread>
#include <tuple>
#include <utility>
#include <vector>

#include "SchedulerContract.h"
#include "ThreadPool.h"

#if __has_include("ThreadPoolV25.h")
#if defined(__clang__)
#pragma clang diagnostic push
#endif
#include "ThreadPoolV25.h"
#if defined(__clang__)
#pragma clang diagnostic pop
#endif
#define BENCH_HAS_V25 1
#else
#define BENCH_HAS_V25 0
#endif

namespace bench {

    using Clock = std::chrono::steady_clock;

    template <class T>
    inline void do_not_optimize(const T& value) noexcept {
#if defined(__GNUC__) || defined(__clang__)
        asm volatile("" : : "r,m"(value) : "memory");
#else
        volatile auto sink = value;
        (void)sink;
#endif
    }

    // ---------------------------------------------------------------- 1. 契约
    template <class P>
    concept TaskScheduler = szz::contract::TaskScheduler<P>;

    template <class P, class F>
    concept DetachedSubmittable = szz::contract::DetachedSubmittable<P, F>;

    template <class P, class F>
    concept FutureSubmittable = szz::contract::FutureSubmittable<P, F>;

    // 只有带仪表盘的池才满足；将来某个臂没有也不影响编译
    template <class P>
    concept HasStats = szz::contract::HasStats<P>;

    template <class P>
    concept HasBatch = szz::contract::BatchSubmittable<P>;

    template <class P>
    concept HasBatchStats = szz::contract::HasBatchStats<P>;

    static_assert(TaskScheduler<szz::ThreadPool>);
    static_assert(DetachedSubmittable<szz::ThreadPool, void (*)()>);
    static_assert(FutureSubmittable<szz::ThreadPool, void (*)()>);
    static_assert(HasStats<szz::ThreadPool>, "基线必须带 park_count / max_queue_depth");

#if BENCH_HAS_V25
    static_assert(TaskScheduler<szz::batch::ThreadPool>);
    static_assert(DetachedSubmittable<szz::batch::ThreadPool, void (*)()>);
    static_assert(FutureSubmittable<szz::batch::ThreadPool, void (*)()>);
    static_assert(HasStats<szz::batch::ThreadPool>);
    static_assert(HasBatch<szz::batch::ThreadPool>);
    static_assert(HasBatchStats<szz::batch::ThreadPool>);
#endif

    // ---------------------------------------------------------------- 2. 负载
    struct Buffers {
        std::vector<double> a, b, c;
        explicit Buffers(std::size_t n) : a(n, 1.5), b(n, 2.0), c(n, 0.0) {}
    };

    inline void elementwise(const Buffers& in, Buffers& out, std::size_t b, std::size_t e)
    {
        for (std::size_t i = b; i < e; ++i)
            out.c[i] = in.a[i] * in.b[i] + 1.0;
    }

    // ---------------------------------------------------------------- 3. 用例与样本
    struct Point {
        std::size_t threads;      // 消费者线程数
        std::size_t producers;    // 1 = 普通单生产者
        std::size_t n_tasks;
    };

    struct Sample {
        double      ms{ 0.0 };
        std::size_t parks{ 0 };   // 0 = 该臂没有仪表盘
        std::size_t depth{ 0 };
        std::size_t acq{ 0 };
        double      avg_take{ 0.0 };
        std::size_t s_depth{ 0 };
    };

    // ---------------------------------------------------------------- 4. 一次测量
    template <TaskScheduler Pool>
    Sample measure_once(const Point& pt, const Buffers& in, Buffers& out)
    {
        const std::size_t n = in.a.size();
        const std::size_t per = (n + pt.n_tasks - 1) / pt.n_tasks;

        std::vector<std::pair<std::size_t, std::size_t>> ranges;
        ranges.reserve(pt.n_tasks);
        for (std::size_t b = 0; b < n; b += per)
            ranges.emplace_back(b, std::min(b + per, n));

        std::fill(out.c.begin(), out.c.end(), 0.0);
        Pool pool(pt.threads);                    // 计时区外

        const auto t0 = Clock::now();

        if (pt.producers <= 1) {
            if constexpr (HasBatch<Pool>) {
                auto batch = pool.make_batch();
                for (const auto& r : ranges) {
                    const std::size_t b = r.first, e = r.second;
                    batch.submit([&in, &out, b, e] { elementwise(in, out, b, e); });
                }
            }
            else {
                for (const auto& r : ranges) {
                    const std::size_t b = r.first, e = r.second;
                    pool.enqueue_detached([&in, &out, b, e] { elementwise(in, out, b, e); });
                }
            }
        }
        else {
            std::vector<std::jthread> producers;
            producers.reserve(pt.producers);
            for (std::size_t p = 0; p < pt.producers; ++p) {
                producers.emplace_back([&, p] {
                    if constexpr (HasBatch<Pool>) {
                        auto batch = pool.make_batch();
                        for (std::size_t i = p; i < ranges.size(); i += pt.producers) {
                            const std::size_t b = ranges[i].first, e = ranges[i].second;
                            batch.submit([&in, &out, b, e] { elementwise(in, out, b, e); });
                        }
                    }
                    else {
                        for (std::size_t i = p; i < ranges.size(); i += pt.producers) {
                            const std::size_t b = ranges[i].first, e = ranges[i].second;
                            pool.enqueue_detached([&in, &out, b, e] { elementwise(in, out, b, e); });
                        }
                    }
                    });
            }
        }

        pool.wait_all();
        const auto t1 = Clock::now();

        double checksum = 0.0;
        for (std::size_t i = 0; i < out.c.size(); ++i) {
            if (std::abs(out.c[i] - 4.0) > 1e-9) {
                std::cerr << "\n[VALIDATION ERROR] elementwise mismatch at index " << i
                          << " (threads=" << pt.threads << ", producers=" << pt.producers
                          << ", n_tasks=" << pt.n_tasks << "): expected 4.0, got "
                          << out.c[i] << '\n';
                std::exit(1);
            }
            checksum += out.c[i];
        }
        do_not_optimize(checksum);

        Sample s;
        s.ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

        if constexpr (HasStats<Pool>) {            // 计时区外读取
            s.parks = pool.park_count();
            s.depth = pool.max_queue_depth();
        }

        if constexpr (HasBatchStats<Pool>) {
            s.acq = pool.consumer_acquisitions();
            s.avg_take = pool.avg_take();
            s.s_depth = pool.steady_max_depth();
        }
        return s;
    }

    // ---------------------------------------------------------------- 5. 统计
    struct Stats {
        std::size_t n{ 0 };
        double median{ 0 }, q1{ 0 }, q3{ 0 }, lo{ 0 }, hi{ 0 };

        double iqr()        const { return q3 - q1; }
        double spread_pct() const { return median > 0 ? iqr() / median * 100.0 : 0.0; }
    };

    inline double quantile_sorted(const std::vector<double>& v, double q)
    {
        if (v.empty())     return 0.0;
        if (v.size() == 1) return v.front();
        const double pos = q * static_cast<double>(v.size() - 1);
        const auto   i = static_cast<std::size_t>(pos);
        const auto   j = std::min(i + 1, v.size() - 1);
        const double frac = pos - static_cast<double>(i);
        return v[i] * (1.0 - frac) + v[j] * frac;
    }

    inline Stats summarize(std::vector<double> v)
    {
        if (v.empty()) return {};
        std::sort(v.begin(), v.end());
        return Stats{ v.size(),
                      quantile_sorted(v, 0.50), quantile_sorted(v, 0.25),
                      quantile_sorted(v, 0.75), v.front(), v.back() };
    }

    // ---------------------------------------------------------------- 6. 被测臂
    struct ArmBase {
        static constexpr const char* name = "v2.4";     // 冻结基线（无批量）
        using pool_type = szz::ThreadPool;
    };

    struct ArmTwin {
        static constexpr const char* name = "v2.4-twin";
        using pool_type = szz::ThreadPool;
    };

#if BENCH_HAS_V25
    struct ArmV25 {
        static constexpr const char* name = "v2.5";     // 生产端批量
        using pool_type = szz::batch::ThreadPool;
    };
#endif

    template <class... A> struct ArmList {};

    using Arms = ArmList< ArmBase, ArmTwin
#if BENCH_HAS_V25
        , ArmV25
#endif
    >;

    template <class... A, class F>
    void for_each_arm(ArmList<A...>, F&& f)
    {
        (f.template operator() < A > (), ...);
    }

    struct Record {
        std::string         arm;
        Point               pt;
        std::vector<Sample> samples;    // samples[0] = 预热波
    };

    // ---------------------------------------------------------------- 7. 单臂单点剖析模式 (--profile)
    struct CliOptions {
        bool help{ false };
        bool profile{ false };
        std::string arm{ "v2.4" };
        std::size_t threads{ 8 };
        std::size_t producers{ 1 };
        std::size_t tasks{ 262144 };
        std::size_t waves{ 10 };
    };

    enum class FlagMatch { NoMatch, Matched, Error };

    inline bool parse_positive_size(std::string_view sv, std::size_t& out) noexcept
    {
        if (sv.empty()) return false;
        std::size_t val = 0;
        const char* begin = sv.data();
        const char* end = begin + sv.size();
        const auto [ptr, ec] = std::from_chars(begin, end, val);
        if (ec != std::errc{} || ptr != end || val == 0) return false;
        out = val;
        return true;
    }

    inline FlagMatch match_flag_value(int argc, char** argv, int& idx,
        std::string_view name, std::string_view& out_val, std::string& err)
    {
        const std::string_view arg(argv[idx]);
        if (arg == name) {
            if (idx + 1 >= argc) {
                err = "missing value for " + std::string(name);
                return FlagMatch::Error;
            }
            out_val = argv[++idx];
            if (out_val.empty()) {
                err = "empty value for " + std::string(name);
                return FlagMatch::Error;
            }
            return FlagMatch::Matched;
        }
        if (arg.starts_with(name) && arg.size() > name.size() && arg[name.size()] == '=') {
            out_val = arg.substr(name.size() + 1);
            if (out_val.empty()) {
                err = "empty value for " + std::string(name);
                return FlagMatch::Error;
            }
            return FlagMatch::Matched;
        }
        return FlagMatch::NoMatch;
    }

    inline bool has_flag_args(int argc, char** argv) noexcept
    {
        for (int i = 1; i < argc; ++i) {
            if (std::string_view(argv[i]).starts_with("--")) return true;
        }
        return false;
    }

    inline void print_usage(std::ostream& os, const char* prog)
    {
        os << "Usage:\n"
           << "  " << prog << " [waves [out.csv]]\n"
           << "  " << prog << " --profile [--arm <v2.4|v2.5>] [--threads <T>]"
           << " [--producers <P>] [--tasks <N>] [--waves <W>]\n"
           << "  " << prog << " --help\n\n"
           << "Profile options (no CSV output):\n"
           << "  --help           Show this usage and exit 0\n"
           << "  --profile        Enable single-arm single-point perf isolation mode\n"
           << "  --arm <name>     Scheduler arm: v2.4"
#if BENCH_HAS_V25
           << " or v2.5"
#endif
           << " (default: v2.4)\n"
           << "  --threads <T>    Worker thread count >= 1 (default: 8)\n"
           << "  --producers <P>  Producer thread count >= 1 (default: 1)\n"
           << "  --tasks <N>      Task partition count >= 1 (default: 262144)\n"
           << "  --waves <W>      Measured waves >= 1 (default: 10)\n";
    }

    inline bool parse_cli_args(int argc, char** argv, CliOptions& opts, std::string& err)
    {
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
            { "--threads",   &opts.threads },
            { "--producers", &opts.producers },
            { "--tasks",     &opts.tasks },
            { "--waves",     &opts.waves },
        };

        for (int i = 1; i < argc; ++i) {
            const std::string_view arg(argv[i]);
            if (arg == "--profile") {
                opts.profile = true;
                continue;
            }

            std::string_view val;
            const FlagMatch arm_match = match_flag_value(argc, argv, i, "--arm", val, err);
            if (arm_match == FlagMatch::Error) return false;
            if (arm_match == FlagMatch::Matched) {
                opts.arm = std::string(val);
                continue;
            }

            bool matched_size = false;
            for (const auto& sf : size_flags) {
                const FlagMatch m = match_flag_value(argc, argv, i, sf.name, val, err);
                if (m == FlagMatch::Error) return false;
                if (m == FlagMatch::Matched) {
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
            err = "--profile is required when using -- options";
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

    template <TaskScheduler Pool>
    inline void submit_strided_ranges(Pool& pool, const Buffers& in, Buffers& out,
        const std::vector<std::pair<std::size_t, std::size_t>>& ranges,
        std::size_t start_idx, std::size_t stride)
    {
        if constexpr (HasBatch<Pool>) {
            auto batch = pool.make_batch();
            for (std::size_t i = start_idx; i < ranges.size(); i += stride) {
                const std::size_t b = ranges[i].first, e = ranges[i].second;
                batch.submit([&in, &out, b, e] { elementwise(in, out, b, e); });
            }
            batch.flush();
        }
        else {
            for (std::size_t i = start_idx; i < ranges.size(); i += stride) {
                const std::size_t b = ranges[i].first, e = ranges[i].second;
                pool.enqueue_detached([&in, &out, b, e] { elementwise(in, out, b, e); });
            }
        }
    }

    template <TaskScheduler Pool>
    void run_profile(const char* arm_name, const CliOptions& opts)
    {
        constexpr std::size_t N = 1024;
        Buffers in(N * N), out(N * N);

        const std::size_t n = in.a.size();
        const std::size_t per = (n + opts.tasks - 1) / opts.tasks;
        std::vector<std::pair<std::size_t, std::size_t>> ranges;
        ranges.reserve(opts.tasks);
        for (std::size_t b = 0; b < n; b += per)
            ranges.emplace_back(b, std::min(b + per, n));

        Pool pool(opts.threads);
        double total_ms = 0.0;

        for (std::size_t w = 0; w < opts.waves; ++w) {
            const bool check_wave = (w == 0) || (w + 1 == opts.waves);
            if (check_wave) {
                std::fill(out.c.begin(), out.c.end(), 0.0);
            }

            const auto t0 = Clock::now();
            if (opts.producers <= 1) {
                submit_strided_ranges(pool, in, out, ranges, 0, 1);
            }
            else {
                std::vector<std::jthread> producers;
                producers.reserve(opts.producers);
                for (std::size_t p = 0; p < opts.producers; ++p) {
                    producers.emplace_back([&, p] {
                        submit_strided_ranges(pool, in, out, ranges, p, opts.producers);
                    });
                }
            }
            pool.wait_all();
            const auto t1 = Clock::now();

            total_ms += std::chrono::duration<double, std::milli>(t1 - t0).count();

            if (check_wave) {
                double checksum = 0.0;
                for (std::size_t i = 0; i < out.c.size(); ++i) {
                    if (std::abs(out.c[i] - 4.0) > 1e-9) {
                        std::cerr << "\n[VALIDATION ERROR] elementwise mismatch at index " << i
                                  << " (arm=" << arm_name << ", wave=" << w
                                  << ", threads=" << opts.threads
                                  << ", producers=" << opts.producers
                                  << ", tasks=" << opts.tasks << "): expected 4.0, got "
                                  << out.c[i] << '\n';
                        std::exit(1);
                    }
                    checksum += out.c[i];
                }
                do_not_optimize(checksum);
            }
        }

        const double avg_ms = total_ms / static_cast<double>(opts.waves);
        std::cout << "[profile] arm=" << arm_name
                  << " threads=" << opts.threads
                  << " producers=" << opts.producers
                  << " tasks=" << opts.tasks
                  << " waves=" << opts.waves
                  << std::fixed << std::setprecision(3)
                  << " total_ms=" << total_ms
                  << " avg_ms=" << avg_ms;

        if constexpr (HasStats<Pool>) {
            std::cout << " parks=" << pool.park_count()
                      << " depth=" << pool.max_queue_depth();
        }
        if constexpr (HasBatchStats<Pool>) {
            std::cout << " acq=" << pool.consumer_acquisitions()
                      << " avg_take=" << std::setprecision(1) << pool.avg_take()
                      << " s_depth=" << pool.steady_max_depth();
        }
        std::cout << '\n';
    }

} // namespace bench

// ============================================================================
int main(int argc, char** argv)
{
#ifdef _WIN32
    SetConsoleOutputCP(CP_UTF8);
#endif
    using namespace bench;

    if (has_flag_args(argc, argv)) {
        CliOptions opts;
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
            run_profile<ArmBase::pool_type>(ArmBase::name, opts);
        }
#if BENCH_HAS_V25
        else if (opts.arm == "v2.5") {
            run_profile<ArmV25::pool_type>(ArmV25::name, opts);
        }
#endif
        return 0;
    }

    const std::size_t kWaves = (argc > 1)
        ? static_cast<std::size_t>(std::max(1, std::atoi(argv[1]))) : 7u;
    const char* kOut = (argc > 2) ? argv[2] : "benchmark_results_v2.5.csv";

    constexpr std::size_t N = 1024;
    Buffers in(N * N), out(N * N);

    const std::vector<std::size_t> thread_counts = { 1, 2, 4, 8, 12, 16, 24, 32 };
    const std::vector<std::size_t> task_counts = { 16, 64, 256, 1024, 4096, 16384, 65536, 262144 };
    const std::vector<std::size_t> producer_counts = { 1, 2, 3, 4 };

    std::vector<Point> grid;

    for (std::size_t t : thread_counts)   grid.push_back({ t, 1, 64 });      // A. 线程扩展
    for (std::size_t k : task_counts)     grid.push_back({ 8, 1, k });       // B. 粒度扫描
    for (std::size_t t : { 1, 2, 4, 16 }) grid.push_back({ t, 1, 262144 });  // C. 决定性实验
    for (std::size_t p : producer_counts) grid.push_back({ 8, p, 262144 });  // D. H1

    // 去重：{8,1,64} 与 {8,1,262144} 会同时出现在多组里
    std::sort(grid.begin(), grid.end(), [](Point a, Point b) {
        return std::tie(a.threads, a.producers, a.n_tasks)
            < std::tie(b.threads, b.producers, b.n_tasks); });
    grid.erase(std::unique(grid.begin(), grid.end(), [](Point a, Point b) {
        return a.threads == b.threads && a.producers == b.producers
            && a.n_tasks == b.n_tasks; }), grid.end());

    std::vector<Record> records;
    auto record_index = [&records](const char* arm, Point pt) -> std::size_t {
        for (std::size_t i = 0; i < records.size(); ++i)
            if (records[i].arm == arm && records[i].pt.threads == pt.threads
                && records[i].pt.producers == pt.producers
                && records[i].pt.n_tasks == pt.n_tasks)
                return i;
        records.push_back(Record{ arm, pt,{} });
        return records.size() - 1;
        };

    std::cout << "grid: " << grid.size() << " points x " << kWaves << " waves, "
        << (BENCH_HAS_V25 ? 3 : 2) << " arm(s)\n"
        << "discarding wave 0 as warm-up\n\n";

    for (std::size_t w = 0; w < kWaves; ++w) {
        for (const Point& pt : grid) {
            for_each_arm(Arms{}, [&]<class Arm>() {
                const Sample s = measure_once<typename Arm::pool_type>(pt, in, out);
                records[record_index(Arm::name, pt)].samples.push_back(s);

                std::cout << "  [" << w << "] " << std::setw(8) << std::left << Arm::name
                    << std::right << "  t=" << std::setw(2) << pt.threads
                    << "  prod=" << std::setw(2) << pt.producers
                    << "  n=" << std::setw(6) << pt.n_tasks
                    << "  " << std::setw(9) << std::fixed << std::setprecision(3) << s.ms << " ms";

                if constexpr (HasStats<typename Arm::pool_type>)
                    std::cout << "   park=" << std::setw(7) << s.parks
                    << "  depth=" << std::setw(4) << s.depth;

                std::cout << '\n';
            });
        }
    }

    // ---------------------------------------------------------------- 1. 原始波次宽表 CSV（按臂横向分列）
    {
        std::ofstream csv(kOut);
        if (!csv.is_open()) {
            std::cerr << "\n[ERROR] 无法写入文件: " << kOut
                << " (请先关闭正在打开该文件的 Excel 窗口)\n";
        }
        else {
            // 1.1 动态生成表头：以后在 Arms 中加新臂，列会自动往右追加
            csv << "threads,producers,n_tasks,wave";
            for_each_arm(Arms{}, [&]<class Arm>() { csv << ',' << Arm::name << "_ms"; });
            for_each_arm(Arms{}, [&]<class Arm>() { csv << ',' << Arm::name << "_parks"; });
            for_each_arm(Arms{}, [&]<class Arm>() { csv << ',' << Arm::name << "_depth"; });
            for_each_arm(Arms{}, [&]<class Arm>() {
                if constexpr (HasBatchStats<typename Arm::pool_type>) {
                    csv << ',' << Arm::name << "_acq"
                        << ',' << Arm::name << "_avg_take"
                        << ',' << Arm::name << "_s_depth";
                }
            });
            csv << '\n' << std::fixed << std::setprecision(6);

            // 1.2 按 (Point, wave) 横向对齐输出各臂数据
            for (const Point& pt : grid) {
                for (std::size_t w = 0; w < kWaves; ++w) {
                    csv << pt.threads << ',' << pt.producers << ',' << pt.n_tasks << ',' << w;

                    // 横向并排：各臂耗时 (ms)
                    for_each_arm(Arms{}, [&]<class Arm>() {
                        const auto& s = records[record_index(Arm::name, pt)].samples[w];
                        csv << ',' << s.ms;
                    });
                    // 横向并排：各臂等待次数 (parks)
                    for_each_arm(Arms{}, [&]<class Arm>() {
                        const auto& s = records[record_index(Arm::name, pt)].samples[w];
                        csv << ',' << s.parks;
                    });
                    // 横向并排：各臂最大深度 (depth)
                    for_each_arm(Arms{}, [&]<class Arm>() {
                        const auto& s = records[record_index(Arm::name, pt)].samples[w];
                        csv << ',' << s.depth;
                    });
                    // 横向并排：带批量探针的臂追加 acq / avg_take / s_depth
                    for_each_arm(Arms{}, [&]<class Arm>() {
                        if constexpr (HasBatchStats<typename Arm::pool_type>) {
                            const auto& s = records[record_index(Arm::name, pt)].samples[w];
                            csv << ',' << s.acq << ',' << s.avg_take << ',' << s.s_depth;
                        }
                    });
                    csv << '\n';
                }
            }
            csv.flush();
            std::cout << "\n[CSV] 原始波次横向宽表已写入 -> " << kOut << '\n';
        }
    }

    // ---------------------------------------------------------------- 2. 统计汇总宽表 CSV（每点一行，各臂并排）
    {
        const char* kSummaryOut = "benchmark_summary_wide.csv";
        std::ofstream sum_csv(kSummaryOut);
        if (sum_csv.is_open()) {
            sum_csv << "threads,producers,n_tasks";
            for_each_arm(Arms{}, [&]<class Arm>() { sum_csv << ',' << Arm::name << "_median_ms"; });
            for_each_arm(Arms{}, [&]<class Arm>() { sum_csv << ',' << Arm::name << "_iqr_ms"; });
            for_each_arm(Arms{}, [&]<class Arm>() { sum_csv << ',' << Arm::name << "_spread_pct"; });
            for_each_arm(Arms{}, [&]<class Arm>() { sum_csv << ',' << Arm::name << "_us_task"; });
            for_each_arm(Arms{}, [&]<class Arm>() { sum_csv << ',' << Arm::name << "_parks"; });
            for_each_arm(Arms{}, [&]<class Arm>() { sum_csv << ',' << Arm::name << "_depth"; });
            for_each_arm(Arms{}, [&]<class Arm>() {
                if constexpr (HasBatchStats<typename Arm::pool_type>) {
                    sum_csv << ',' << Arm::name << "_acq"
                        << ',' << Arm::name << "_avg_take"
                        << ',' << Arm::name << "_s_depth";
                }
            });
            sum_csv << '\n' << std::fixed << std::setprecision(6);

            auto calc_stats = [&](const char* arm, Point pt) -> Stats {
                const auto& samps = records[record_index(arm, pt)].samples;
                std::vector<double> v;
                for (std::size_t i = (samps.size() > 1 ? 1 : 0); i < samps.size(); ++i)
                    v.push_back(samps[i].ms);
                return summarize(v);
                };

            for (const Point& pt : grid) {
                sum_csv << pt.threads << ',' << pt.producers << ',' << pt.n_tasks;

                for_each_arm(Arms{}, [&]<class Arm>() {
                    sum_csv << ',' << calc_stats(Arm::name, pt).median;
                });
                for_each_arm(Arms{}, [&]<class Arm>() {
                    sum_csv << ',' << calc_stats(Arm::name, pt).iqr();
                });
                for_each_arm(Arms{}, [&]<class Arm>() {
                    sum_csv << ',' << calc_stats(Arm::name, pt).spread_pct();
                });
                for_each_arm(Arms{}, [&]<class Arm>() {
                    const double med = calc_stats(Arm::name, pt).median;
                    sum_csv << ',' << (med * 1000.0 / static_cast<double>(pt.n_tasks));
                });
                for_each_arm(Arms{}, [&]<class Arm>() {
                    const auto& samps = records[record_index(Arm::name, pt)].samples;
                    sum_csv << ',' << (samps.empty() ? 0 : samps.back().parks);
                });
                for_each_arm(Arms{}, [&]<class Arm>() {
                    const auto& samps = records[record_index(Arm::name, pt)].samples;
                    sum_csv << ',' << (samps.empty() ? 0 : samps.back().depth);
                });
                for_each_arm(Arms{}, [&]<class Arm>() {
                    if constexpr (HasBatchStats<typename Arm::pool_type>) {
                        const auto& samps = records[record_index(Arm::name, pt)].samples;
                        if (samps.empty()) sum_csv << ",0,0,0";
                        else sum_csv << ',' << samps.back().acq
                            << ',' << samps.back().avg_take
                            << ',' << samps.back().s_depth;
                    }
                });
                sum_csv << '\n';
            }
            sum_csv.flush();
            std::cout << "[CSV] 汇总横向宽表已写入 -> " << kSummaryOut << '\n';
        }
    }

    // ---------------------------------------------------------------- 3. 终端汇总打印
    std::cout << "\n=== summary (wave 0 discarded) ===\n"
        << "arm        thr  prd        n     median        IQR   spread   us/task    parks   depth     acq  avg_take  s_depth\n";

    for (const auto& r : records) {
        std::vector<double> v;
        for (std::size_t i = (r.samples.size() > 1 ? 1 : 0); i < r.samples.size(); ++i)
            v.push_back(r.samples[i].ms);

        const Stats s = summarize(v);
        std::cout << std::left << std::setw(8) << r.arm << std::right
            << std::setw(6) << r.pt.threads
            << std::setw(5) << r.pt.producers
            << std::setw(8) << r.pt.n_tasks
            << std::setw(11) << std::fixed << std::setprecision(3) << s.median
            << std::setw(10) << std::setprecision(3) << s.iqr()
            << std::setw(8) << std::setprecision(1) << s.spread_pct() << "%"
            << std::setw(10) << std::setprecision(3)
            << (s.median * 1000.0 / static_cast<double>(r.pt.n_tasks));

        if (!r.samples.empty()) {
            const auto& last = r.samples.back();
            std::cout << std::setw(9) << last.parks
                << std::setw(8) << last.depth
                << std::setw(8) << last.acq
                << std::setw(10) << std::setprecision(1) << last.avg_take
                << std::setw(9) << last.s_depth;
        }
        std::cout << '\n';
    }

    // ---------------------------------------------------------------- H1 汇总
    auto find_record = [&records](const char* arm, Point pt) -> const Record* {
        for (const auto& r : records)
            if (r.arm == arm && r.pt.threads == pt.threads
                && r.pt.producers == pt.producers && r.pt.n_tasks == pt.n_tasks)
                return &r;
        return nullptr;
        };
    auto median_ms = [](const Record& r) {
        std::vector<double> v;
        for (std::size_t i = (r.samples.size() > 1 ? 1 : 0); i < r.samples.size(); ++i)
            v.push_back(r.samples[i].ms);
        return summarize(v).median;
        };

    std::cout << "\n=== H1: 生产者数量扫描 (threads=8, n=262144) ===\n";
    std::cout << "arm       ";
    for (std::size_t p : producer_counts)
        std::cout << ("  k=" + std::to_string(p)).substr(0, 6) << "      ";
    std::cout << "  T_min    T(1)/T_min   容量 M/s\n";

    for_each_arm(Arms{}, [&]<class Arm>() {
        std::vector<double> tv;
        std::cout << std::left << std::setw(8) << Arm::name << std::right;
        for (std::size_t p : producer_counts) {
            const Record* r = find_record(Arm::name, Point{ 8, p, 262144 });
            const double m = r ? median_ms(*r) : 0.0;
            tv.push_back(m);
            if (m > 0) std::cout << std::setw(9) << std::fixed << std::setprecision(3) << m << "  ";
            else       std::cout << std::setw(9) << "-" << "  ";
        }
        double tmin = 1e18;
        for (double v : tv) if (v > 0.0) tmin = std::min(tmin, v);
        if (tmin < 1e17) {
            const double ratio = tv[0] > 0.0 ? tv[0] / tmin : 0.0;
            const double cap = 262.144 / tmin;              // 262144 任务 / tmin(ms) → M/s
            std::cout << std::setw(9) << std::setprecision(3) << tmin
                << std::setw(12) << std::setprecision(2) << ratio << "x"
                << std::setw(12) << std::setprecision(2) << cap;
        }
        std::cout << '\n';
    });

    std::cout << "\n判读：\n"
        << "  T(1)/T_min ≈ 1  → 多生产者无收益，瓶颈不在提交路径\n"
        << "  T(1)/T_min ≈ N  → 生产者是瓶颈；容量列即消费者上限\n"
        << "  k 增大后 T 反弹 → 单一全局锁的争用超过并行收益\n";

    std::cout << "\n[check] 基线 v2.4 目标：t=8 / prod=1 / n=262144 的 median ≈ 180.6 ms\n";
    return 0;
}