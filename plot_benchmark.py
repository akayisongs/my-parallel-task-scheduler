import pandas as pd
import matplotlib.pyplot as plt
import numpy as np

# 设置全局字体与高分辨率学术/工业图表风格
plt.rcParams['font.sans-serif'] = ['Microsoft YaHei', 'SimHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False
plt.rcParams['figure.dpi'] = 300

# 读取 v2.3 压测数据
df = pd.read_csv('benchmark_results_v2.3.csv')

# 拆分实验 1 (scaling) 与 实验 2 (granularity)
df_scaling = df[df['experiment'] == 'scaling'].copy()
df_gran = df[df['experiment'] == 'granularity'].copy()

# 以 Threads = 1 的实测耗时 (272.385 ms) 作为真实单核基准 T1 重新校准加速比
t1_ms = df_scaling.loc[df_scaling['param'] == 1, 'time_ms'].values[0]
df_scaling['true_speedup'] = t1_ms / df_scaling['time_ms']

# 创建 1x2 并排双子图
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 5.5))
fig.patch.set_facecolor('#FFFFFF')

# ==========================================
# 子图 1：线程扩展性与多核加速比曲线 (Strong Scaling)
# ==========================================
threads = df_scaling['param'].astype(str)
times = df_scaling['time_ms']
speedups = df_scaling['true_speedup']

color_bar = '#3B82F6'   # 现代科技蓝
color_line = '#EF4444'  # 醒目珊瑚红

bars = ax1.bar(threads, times, color=color_bar, alpha=0.85, width=0.5, label='执行耗时 (ms)')
ax1.set_xlabel('工作线程数 (Worker Threads)', fontsize=11, fontweight='bold')
ax1.set_ylabel('平均计算耗时 (ms) [越低越好]', color=color_bar, fontsize=11, fontweight='bold')
ax1.tick_params(axis='y', labelcolor=color_bar)
ax1.set_ylim(0, max(times) * 1.18)
ax1.grid(axis='y', linestyle='--', alpha=0.4)

# 在柱状图顶部标注具体毫秒数
for bar in bars:
    h = bar.get_height()
    ax1.text(bar.get_x() + bar.get_width()/2., h + 5, f'{h:.1f}ms',
             ha='center', va='bottom', fontsize=8.5, color='#1E3A8A')

# 右轴：加速比折线
ax1_right = ax1.twinx()
line = ax1_right.plot(threads, speedups, color=color_line, marker='o', linewidth=2.5, 
                      markersize=7, label='真实加速比 (Speedup = T1 / Tp)')
ax1_right.set_ylabel('多核加速比 (Speedup) [越高越好]', color=color_line, fontsize=11, fontweight='bold')
ax1_right.tick_params(axis='y', labelcolor=color_line)
ax1_right.set_ylim(0, max(speedups) * 1.2)

# 标注加速比数值
for i, sp in enumerate(speedups):
    ax1_right.annotate(f'{sp:.2f}x', (threads.iloc[i], sp),
                       textcoords="offset points", xytext=(0, 8),
                       ha='center', fontsize=9, fontweight='bold', color=color_line)

ax1.set_title('实验 1: GEMM (1024x1024) 线程扩展性与加速比', fontsize=12, fontweight='bold', pad=12)

# ==========================================
# 子图 2：微任务切分粒度与全局锁竞争崩盘曲线 (Log-Log)
# ==========================================
tasks = df_gran['param']
gran_times = df_gran['time_ms']

ax2.plot(tasks, gran_times, color='#8B5CF6', marker='s', linewidth=2.5, markersize=7,
         label='总耗时 (含调度与锁竞争开销)')
ax2.set_xscale('log', base=2)
ax2.set_yscale('log', base=10)
ax2.set_xlabel('切分微任务总数 (Total Tasks, Log2 Scale)', fontsize=11, fontweight='bold')
ax2.set_ylabel('总执行耗时 (ms, Log10 Scale)', fontsize=11, fontweight='bold')
ax2.grid(True, which="both", linestyle='--', alpha=0.4)

# 划分“计算主导区”与“锁竞争线性崩盘区”分界线
ax2.axvline(x=1024, color='#F59E0B', linestyle='--', linewidth=1.8, label='锁竞争拐点 (1024 Tasks)')
ax2.axvspan(1024, 262144, color='#FEE2E2', alpha=0.35, label='全局锁与堆分配主导区 (~1.75 μs/task)')

# 标注每个点的耗时
for x, y in zip(tasks, gran_times):
    ax2.annotate(f'{y:.2f}ms', (x, y), textcoords="offset points", xytext=(0, 8),
                 ha='center', fontsize=8.5, fontweight='bold', color='#4C1D95')

ax2.set_title('实验 2: 任务粒度与全局互斥锁竞争压测 (8 Threads)', fontsize=12, fontweight='bold', pad=12)
ax2.legend(loc='upper left', fontsize=9.5)

plt.tight_layout()
plt.savefig('benchmark_v2.3_charts.png', dpi=300)
print("图表已成功生成：benchmark_v2.3_charts.png")