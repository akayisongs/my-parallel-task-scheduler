// ============================================================================
//  bench_scheduler.cpp — 同进程 A/B 压测框架
//  设计约束：
//    * 计时区间 = 派发 + wait_all，不含建池、不含 join
//    * 每点 N 次独立采样，报 median + IQR，丢第 0 波
//    * 外层按波次循环，同一点的各臂背靠背测量，抵消环境漂移
//    * 被测臂通过模板参数注入，调度器作为 TaskScheduler 概念传入
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
#include <utility>
#include <vector>
#include <windows.h>

#include "ThreadPool.h"

#if __has_include("ThreadPoolV2.h")
#include "ThreadPoolV2.h"
#define BENCH_HAS_V2 1
#else
#define BENCH_HAS_V2 0
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

    static_assert(TaskScheduler<szz::ThreadPool>);
    static_assert(DetachedSubmittable<szz::ThreadPool, void (*)()>);

    // ---------------------------------------------------------------- 2. 负载
    struct Buffers {
        std::vector<double> a, b, c;
        explicit Buffers(std::size_t n) : a(n, 1.5), b(n, 2.0), c(n, 0.0) {}
    };

    template <TaskScheduler Pool>
    void submit_elementwise(Pool& pool, const Buffers& in, Buffers& out, std::size_t n_tasks)
    {
        const std::size_t n = in.a.size();
        const std::size_t per = (n + n_tasks - 1) / n_tasks;

        for (std::size_t begin = 0; begin < n; begin += per) {
            const std::size_t end = std::min(begin + per, n);
            pool.enqueue_detached([&in, &out, begin, end] {
                for (std::size_t i = begin; i < end; ++i)
                    out.c[i] = in.a[i] * in.b[i] + 1.0;
                });
        }
    }

    // ---------------------------------------------------------------- 3. 一次测量
    template <class Pool>
    double measure_once(std::size_t threads, std::size_t n_tasks,
        const Buffers& in, Buffers& out)
    {
        Pool pool(threads);                       // 计时区外

        const auto t0 = Clock::now();
        submit_elementwise(pool, in, out, n_tasks);
        pool.wait_all();
        const auto t1 = Clock::now();

        return std::chrono::duration<double, std::milli>(t1 - t0).count();
    }

    // ---------------------------------------------------------------- 4. 统计
    struct Stats {
        std::size_t n{ 0 };
        double median{ 0 }, q1{ 0 }, q3{ 0 }, lo{ 0 }, hi{ 0 };

        double iqr()        const { return q3 - q1; }
        double spread_pct() const { return median > 0 ? iqr() / median * 100.0 : 0.0; }
    };

    inline double quantile_sorted(const std::vector<double>& v, double q)
    {
        if (v.empty())  return 0.0;
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

    // ---------------------------------------------------------------- 5. 被测臂
    struct ArmV24 {
        static constexpr const char* name = "v2.4-global";
        using pool_type = szz::ThreadPool;
    };

#if BENCH_HAS_V2
    struct ArmV30 {
        static constexpr const char* name = "v3.0-ws";
        using pool_type = szz::ThreadPoolV2;       // 改成你实际的类名
    };
#endif

    template <class... A> struct ArmList {};

    using Arms = ArmList< ArmV24
#if BENCH_HAS_V2
        , ArmV30
#endif
    >;

    template <class... A, class F>
    void for_each_arm(ArmList<A...>, F&& f)
    {
        (f.template operator() < A > (), ...);          // C++20 折叠表达式 + 模板 lambda
    }

    // ---------------------------------------------------------------- 6. driver
    struct Point { std::size_t threads; std::size_t n_tasks; };

    struct Record {
        std::string         arm;
        Point               pt;
        std::vector<double> samples;                // samples[0] = 预热波，统计时丢弃
    };

} // namespace bench

int main(int argc, char** argv)
{
    SetConsoleOutputCP(CP_UTF8);
    using namespace bench;

    const std::size_t kWaves = (argc > 1) ? static_cast<std::size_t>(std::max(1, std::atoi(argv[1]))) : 7u;
    const char* kOut = (argc > 2) ? argv[2] : "bench_baseline.csv";

    constexpr std::size_t N = 1024;
    Buffers in(N * N), out(N * N);

    const std::vector<std::size_t> thread_counts = { 1, 2, 4, 8, 12, 16, 24, 32 };
    const std::vector<std::size_t> task_counts = { 16, 64, 256, 1024, 4096, 16384, 65536, 262144 };

    std::vector<Point> grid;
    for (std::size_t t : thread_counts) grid.push_back({ t, 64 });        // 复刻实验 1
    for (std::size_t k : task_counts)   grid.push_back({ 8, k });        // 复刻实验 2
    for (std::size_t t : { 1, 2, 4, 16 }) grid.push_back({ t, 262144 });  // ★ 决定性实验

    std::vector<Record> records;
    auto record_index = [&records](const char* arm, Point pt) -> std::size_t {
        for (std::size_t i = 0; i < records.size(); ++i)
            if (records[i].arm == arm && records[i].pt.threads == pt.threads
                && records[i].pt.n_tasks == pt.n_tasks)
                return i;
        records.push_back(Record{ arm, pt,{} });
        return records.size() - 1;
        };

    std::cout << "grid: " << grid.size() << " points x " << kWaves << " waves\n"
        << "discarding wave 0 as warm-up\n\n";

    for (std::size_t w = 0; w < kWaves; ++w) {
        for (const Point& pt : grid) {
            for_each_arm(Arms{}, [&]<class Arm>() {
                const double ms = measure_once<typename Arm::pool_type>(
                    pt.threads, pt.n_tasks, in, out);
                records[record_index(Arm::name, pt)].samples.push_back(ms);

                std::cout << "  [" << w << "] " << std::setw(12) << std::left << Arm::name
                    << std::right << "  t=" << std::setw(2) << pt.threads
                    << "  n=" << std::setw(6) << pt.n_tasks
                    << "  " << std::setw(9) << std::fixed << std::setprecision(3) << ms
                    << " ms\n";
            });
        }
    }

    std::ofstream csv(kOut);
    csv << "arm,threads,n_tasks,wave,ms\n" << std::setprecision(6);
    for (const auto& r : records)
        for (std::size_t w = 0; w < r.samples.size(); ++w)
            csv << r.arm << ',' << r.pt.threads << ',' << r.pt.n_tasks << ','
            << w << ',' << r.samples[w] << '\n';

    std::cout << "\n=== summary (wave 0 discarded) ===\n"
        << "arm            thr       n     median       IQR   spread   us/task\n";
    for (const auto& r : records) {
        std::vector<double> v(r.samples.begin() + (r.samples.size() > 1 ? 1 : 0),
            r.samples.end());
        const Stats s = summarize(v);
        std::cout << std::left << std::setw(14) << r.arm << std::right
            << std::setw(4) << r.pt.threads
            << std::setw(8) << r.pt.n_tasks
            << std::setw(11) << std::fixed << std::setprecision(3) << s.median
            << std::setw(10) << std::setprecision(3) << s.iqr()
            << std::setw(8) << std::setprecision(1) << s.spread_pct() << "%"
            << std::setw(10) << std::setprecision(3)
            << (s.median * 1000.0 / static_cast<double>(r.pt.n_tasks))
            << '\n';
    }

    std::cout << "\n[check] 目标：v2.4-global / t=8 / n=262144 的 median ≈ 246.9 ms\n";
    return 0;
}