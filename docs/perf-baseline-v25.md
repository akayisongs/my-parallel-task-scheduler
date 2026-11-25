# v2.4 / v2.5 Linux perf 硬件计数器基线档案

> **原始日志**：[`data/perf_v25.log`](../data/perf_v25.log)
> **关联决策**：[`ADR-0001`](adr/0001-freeze-v25-shelve-v26-pivot-to-v30.md)、[`ADR-0002`](adr/0002-fixed-batch-64-imbalance-motivates-v30.md)
> **定位**：在 8 颗独立物理核（CPU `0,2,4,6,8,10,12,14`）与 `--profile` 单点隔离模式下采集并固化 `v2.4` / `v2.5` 的 Linux `perf` 硬件计数器基线，作为后续 `v3.0`（Work-Stealing + `alignas(64)` + DAG）同口径 A/B 对照基准。

---

## 一、 核心硬件计数器对照总表（8 独立物理核：`0,2,4,6,8,10,12,14`）

| 负载与版本 | 墙钟时间 (s) | CPUs utilized (满分 8) | Context Switches | Retired Instructions | IPC | `cycles / ref-cycles` | L1D Load Misses (率) | LLC Misses (率) |
| :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- | :--- |
| 微任务 `v2.4` (`n=262144 x 10`) | 2.136 | **4.80** | 1,883,185 | 18.68 B | 0.56 | 1.245 | 1,013.3 M (14.83%) | 169.4 M (8.16%) |
| 微任务 `v2.5` (`n=262144 x 10`) | 0.406 (**5.26x**) | **2.96** | 94,990 (-95%) | 3.24 B (-83%) | 0.67 | 1.239 | 149.7 M (12.16%) | 29.3 M (12.27%) |
| GEMM `v2.4` (`1024, 64 tasks x 5`) | 0.123 | **6.78** | 120 | 17.59 B | 5.10 | 1.094 | 686.2 M (8.49%) | 8.02 M (0.42%) |
| GEMM `v2.5` (`1024, 64 tasks x 5`) | 0.743 (**慢 6.05x**) | **1.00** | 54 | 17.59 B | 5.19 | 1.204 | 685.1 M (8.48%) | 7.34 M (0.37%) |

### 口径与辅助探针明细

- 所有数据均为 `perf stat -r 5`（5 次运行均值）；为避免 PMU 计数器时分复用（multiplexing）引入误差，`core` 组与 `cache` 组事件分两次独立采集。
- 衍生指标计算口径：
  - `CPUs utilized = task-clock / time elapsed`（来自 `core` 组）
  - `IPC = instructions / cycles`
  - `cycles / ref-cycles`：有效睿频 / 标称参考时钟比值代理
  - `L1D Load Misses (率) = l1-dcache-load-misses / l1-dcache-loads`
  - `LLC Misses (率) = cache-misses / cache-references`

| 负载与版本 | `task-clock` (ms) | `cpu-migrations` | `cycles` | `ref-cycles` | `[profile]` 单轮 `avg_ms` 区间 | `[profile]` 内部调度探针（`core` 组） |
| :--- | ---: | ---: | ---: | ---: | :--- | :--- |
| 微任务 `v2.4` | 10,260.03 | 271,349 | 33.41 B | 26.84 B | 196.905–231.201 ms | `parks=1,479,008–1,846,646`, `depth=2,574–29,391` |
| 微任务 `v2.5` | 1,201.05 | 6,215 | 4.80 B | 3.87 B | 37.842–39.640 ms | `parks=40,894–40,920`, `acq=40,960`, `avg_take=64.0`, `s_depth=384–576` |
| GEMM `v2.4` | 832.08 | 45 | 3.45 B | 3.15 B | 22.002–23.719 ms | `parks=48`, `depth=61–64` |
| GEMM `v2.5` | 742.20 | 28 | 3.39 B | 2.81 B | 144.153–150.116 ms | `parks=13`, `depth=64`, `acq=5`, `avg_take=64.0`, `s_depth=0` |

---

## 二、 硬件计数器归因摘要

1. **微任务（`n=262144 x 10`）**：`v2.5` 两端批量化将墙钟时间从 `2.136 s` 压降至 `0.406 s`（**5.26x**），`context-switches` 下降 `95%`（`1,883,185 -> 94,990`），`cpu-migrations` 下降 `97.7%`（`271,349 -> 6,215`），`instructions` 下降 `83%`（`18.68 B -> 3.24 B`），`l1-dcache-load-misses` 与 `cache-misses` 绝对次数分别下降 `85.2%` 与 `82.7%`。但受限于单一全局互斥锁内的串行队列搬运，8 核利用率仍只有 `2.96 / 8`、`IPC = 0.67`。
2. **粗粒度 GEMM（`1024, 64 tasks x 5`）**：`v2.4` 与 `v2.5` 的退休指令数完全一致（`17.59 B`），`IPC`（`5.10` vs `5.19`）、L1D 失效率（`8.49%` vs `8.48%`）与 LLC 失效率（`0.42%` vs `0.37%`）几乎相同，说明计算内核与单核局部性并无差异；但 `v2.5` 的 `CPUs utilized` 从 `6.78` 跌至 `1.00`，墙钟时间反慢 **6.05x**（`0.123 s -> 0.743 s`）。结合内部探针 `acq=5, avg_take=64.0`（5 轮共取批 5 次，每轮 1 个工作线程一次领走全部 64 个任务，其余 7 核空转），直接坐实了 [`ADR-0002`](adr/0002-fixed-batch-64-imbalance-motivates-v30.md) 记录的固定批大小 `64` 负载不均问题。

---

## 三、 测试环境与 `lscpu` 物理核映射

| 项目 | 实测配置 |
| :--- | :--- |
| CPU 型号与拓扑 | `AMD Ryzen 7 8845H w/ Radeon(TM) 780M Graphics`（`x86_64`，1 Socket，8 物理核，2 线程/核，16 逻辑 CPU，最高睿频 `5137.9038 MHz`） |
| 缓存层级 | `L1d` 256 KiB (8 instances)、`L1i` 256 KiB (8 instances)、`L2` 8 MiB (8 instances)、`L3` 16 MiB (1 instance) |
| 内核版本 | `Linux 7.3.5-zen1-1-zen` |
| 编译器与构建参数 | `clang version 23.1.5`（`RelWithDebInfo`：`-O2 -g -DNDEBUG -std=c++23 -Wall -Wextra -fno-omit-frame-pointer`，`SCHEDULER_SANITIZER=NONE`） |
| 剖析工具 | `perf version 7.3.6-1` |

### `lscpu` 物理核映射与绑核理由

执行以下指令查看当前主机的逻辑 CPU 与物理 `CORE` 对应关系：

```bash
lscpu -e=CPU,CORE,SOCKET,ONLINE,MAXMHZ
```

在本机（单 Socket、每物理核 2 个 SMT 逻辑线程）上的映射如下：

| 物理核 `CORE` | 同核 SMT 逻辑 CPU 对 | `taskset` 选用 CPU |
| :---: | :---: | :---: |
| `0` | `0, 1` | `0` |
| `1` | `2, 3` | `2` |
| `2` | `4, 5` | `4` |
| `3` | `6, 7` | `6` |
| `4` | `8, 9` | `8` |
| `5` | `10, 11` | `10` |
| `6` | `12, 13` | `12` |
| `7` | `14, 15` | `14` |

每颗物理核仅选取 1 个逻辑线程（`0,2,4,6,8,10,12,14`）执行 `taskset`，可消除 `threads=8` 压测时线程被调度到同一物理核 SMT 兄弟线程上造成的执行单元与 L1/L2 缓存资源竞争，保证 8 线程真独占 8 颗物理核心。

---

## 四、 关键复现命令

### 1. 配置并构建 `build-perf`

```bash
cmake -S . -B build-perf -G Ninja \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DCMAKE_CXX_COMPILER=clang++ \
  -DSCHEDULER_SANITIZER=NONE

cmake --build build-perf --target bench_scheduler gemm_benchmark
```

### 2. 确认物理核掩码并复现 `data/perf_v25.log` 全量采样

```bash
# 1) 检查物理核映射（确认每核第一个逻辑 CPU 为 0,2,4,6,8,10,12,14）
lscpu -e=CPU,CORE,SOCKET,ONLINE,MAXMHZ

CPUS="0,2,4,6,8,10,12,14"
CORE_EVENTS="task-clock,context-switches,cpu-migrations,cycles,ref-cycles,instructions"
CACHE_EVENTS="cache-references,cache-misses,L1-dcache-loads,L1-dcache-load-misses"

{
  echo "=== microtask v2.4 (core) ==="
  taskset -c "$CPUS" perf stat -r 5 -e "$CORE_EVENTS" \
    ./build-perf/bench_scheduler --profile --arm v2.4 --threads 8 --producers 1 --tasks 262144 --waves 10

  echo "=== microtask v2.4 (cache) ==="
  taskset -c "$CPUS" perf stat -r 5 -e "$CACHE_EVENTS" \
    ./build-perf/bench_scheduler --profile --arm v2.4 --threads 8 --producers 1 --tasks 262144 --waves 10

  echo "=== microtask v2.5 (core) ==="
  taskset -c "$CPUS" perf stat -r 5 -e "$CORE_EVENTS" \
    ./build-perf/bench_scheduler --profile --arm v2.5 --threads 8 --producers 1 --tasks 262144 --waves 10

  echo "=== microtask v2.5 (cache) ==="
  taskset -c "$CPUS" perf stat -r 5 -e "$CACHE_EVENTS" \
    ./build-perf/bench_scheduler --profile --arm v2.5 --threads 8 --producers 1 --tasks 262144 --waves 10

  echo "=== gemm v2.4 (core) ==="
  taskset -c "$CPUS" perf stat -r 5 -e "$CORE_EVENTS" \
    ./build-perf/gemm_benchmark --profile --arm v2.4 --threads 8 --size 1024 --rows 16 --waves 5

  echo "=== gemm v2.4 (cache) ==="
  taskset -c "$CPUS" perf stat -r 5 -e "$CACHE_EVENTS" \
    ./build-perf/gemm_benchmark --profile --arm v2.4 --threads 8 --size 1024 --rows 16 --waves 5

  echo "=== gemm v2.5 (core) ==="
  taskset -c "$CPUS" perf stat -r 5 -e "$CORE_EVENTS" \
    ./build-perf/gemm_benchmark --profile --arm v2.5 --threads 8 --size 1024 --rows 16 --waves 5

  echo "=== gemm v2.5 (cache) ==="
  taskset -c "$CPUS" perf stat -r 5 -e "$CACHE_EVENTS" \
    ./build-perf/gemm_benchmark --profile --arm v2.5 --threads 8 --size 1024 --rows 16 --waves 5
} 2>&1 | tee data/perf_v25.log
```

### 3. 后续 `v3.0` 同口径对照约定

后续在接入 `v3.0`（Work-Stealing + `alignas(64)` + DAG）进行性能归因时，应保持相同的 `CPUS="0,2,4,6,8,10,12,14"` 绑核掩码、`--profile` 负载参数，以及分 `core` / `cache` 两组 `perf stat -r 5` 采样方式，避免 PMU 事件复用与跨核/SMT 口径漂移。
