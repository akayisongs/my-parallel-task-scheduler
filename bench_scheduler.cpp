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
#ifndef NOMINMAX
#define NOMINMAX
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif

#include <algorithm>
#include <chrono>
#include <concepts>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <string>
#include <thread>
#include <tuple>
#include <utility>
#include <vector>
#include <windows.h>

#include "ThreadPool.h"

#if __has_include("ThreadPoolV25.h")
#include "ThreadPoolV25.h"
#define BENCH_HAS_V25 1
#else
#define BENCH_HAS_V25 0
#endif

namespace bench {

    using Clock = std::chrono::steady_clock;

    // ---------------------------------------------------------------- 1. 契约
    template <class P>
    concept TaskScheduler = requires(P & p, const P & cp) {
        { p.wait_all() }        -> std::same_as<void>;
        { cp.pending_tasks() }  noexcept -> std::same_as<std::size_t>;
        { cp.is_stopped() }     noexcept -> std::same_as<bool>;
    };

    template <class P, class F>
    concept DetachedSubmittable = requires(P & p, F && f) {
        p.enqueue_detached(std::forward<F>(f));
    };

    // 只有带仪表盘的池才满足；将来某个臂没有也不影响编译
    template <class P>
    concept HasStats = requires(const P & p) {
        { p.park_count() }      noexcept -> std::same_as<std::size_t>;
        { p.max_queue_depth() } noexcept -> std::same_as<std::size_t>;
    };

    static_assert(TaskScheduler<szz::ThreadPool>);
    static_assert(DetachedSubmittable<szz::ThreadPool, void (*)()>);
    static_assert(HasStats<szz::ThreadPool>, "基线必须带 park_count / max_queue_depth");

#if BENCH_HAS_V25
    static_assert(TaskScheduler<szz::v25::ThreadPool>);
    static_assert(DetachedSubmittable<szz::v25::ThreadPool, void (*)()>);
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

        Pool pool(pt.threads);                    // 计时区外

        const auto t0 = Clock::now();

        if (pt.producers <= 1) {
            for (const auto& r : ranges) {
                const std::size_t b = r.first, e = r.second;
                pool.enqueue_detached([&in, &out, b, e] { elementwise(in, out, b, e); });
            }
        }
        else {
            std::vector<std::jthread> producers;
            producers.reserve(pt.producers);
            for (std::size_t p = 0; p < pt.producers; ++p) {
                producers.emplace_back([&, p] {
                    for (std::size_t i = p; i < ranges.size(); i += pt.producers) {
                        const std::size_t b = ranges[i].first, e = ranges[i].second;
                        pool.enqueue_detached([&in, &out, b, e] { elementwise(in, out, b, e); });
                    }
                    });
            }
        }                                          // producers 析构 → join，计入计时

        pool.wait_all();
        const auto t1 = Clock::now();

        Sample s;
        s.ms = std::chrono::duration<double, std::milli>(t1 - t0).count();

        if constexpr (HasStats<Pool>) {            // 计时区外读取
            s.parks = pool.park_count();
            s.depth = pool.max_queue_depth();
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

#if BENCH_HAS_V25
    struct ArmV25 {
        static constexpr const char* name = "v2.5";     // 生产端批量
        using pool_type = szz::v25::ThreadPool;
    };
#endif

    template <class... A> struct ArmList {};

    using Arms = ArmList< ArmBase
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

} // namespace bench

// ============================================================================
int main(int argc, char** argv)
{
    SetConsoleOutputCP(CP_UTF8);
    using namespace bench;

    const std::size_t kWaves = (argc > 1)
        ? static_cast<std::size_t>(std::max(1, std::atoi(argv[1]))) : 7u;
    const char* kOut = (argc > 2) ? argv[2] : "bench_baseline.csv";

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
        << (BENCH_HAS_V25 ? 2 : 1) << " arm(s)\n"
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

    // ---------------------------------------------------------------- CSV
    std::ofstream csv(kOut);
    csv << "arm,threads,producers,n_tasks,wave,ms,parks,max_depth\n" << std::setprecision(6);
    for (const auto& r : records)
        for (std::size_t w = 0; w < r.samples.size(); ++w)
            csv << r.arm << ',' << r.pt.threads << ',' << r.pt.producers << ','
            << r.pt.n_tasks << ',' << w << ',' << r.samples[w].ms << ','
            << r.samples[w].parks << ',' << r.samples[w].depth << '\n';

    // ---------------------------------------------------------------- 汇总
    std::cout << "\n=== summary (wave 0 discarded) ===\n"
        << "arm        thr  prd       n     median       IQR   spread   us/task   parks   depth\n";

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

        if (!r.samples.empty())
            std::cout << std::setw(9) << r.samples.back().parks
            << std::setw(8) << r.samples.back().depth;
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