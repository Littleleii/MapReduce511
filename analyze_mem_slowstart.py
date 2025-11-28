import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import os
import re
import datetime
from matplotlib import rcParams
from scipy.interpolate import make_interp_spline

# --- 1. 全局样式配置 (保持您喜欢的宽屏风格) ---
config = {
    "font.family": 'serif',
    "font.size": 12,
    "mathtext.fontset": 'stix',
    "font.serif": ['SimHei'],
}
rcParams.update(config)
plt.rcParams['axes.unicode_minus'] = False
sns.set_context("paper", font_scale=1.2)
sns.set_style("whitegrid", {"font.sans-serif": ['SimHei', 'Microsoft YaHei']})
plt.rcParams['figure.dpi'] = 300


# --- 2. 解析函数：内存 (MEM) ---
def parse_monitor_mem(log_path):
    if not os.path.exists(log_path): return pd.DataFrame()
    with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
        lines = f.readlines()
    data = []
    t = -1
    for line in lines:
        if '----' in line:
            t += 1;
            continue
        # 提取 MEM 数据
        match = re.match(r'\[(\w+-\w+)\] CPU: (\d+\.\d+)% \| MEM: (\d+)%', line)
        if match:
            data.append({'Time_Step': t, 'Node': match.group(1), 'MEM': int(match.group(3))})
    return pd.DataFrame(data)


# --- 3. 解析函数：进度 (Map/Reduce %) ---
def parse_progress(log_path):
    if not os.path.exists(log_path): return pd.DataFrame()
    with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.readlines()

    progress_data = []
    first_ts = None

    # 正则匹配: 2025-11-27 23:31:23,628 ... map 1% reduce 0%
    p = re.compile(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),.* map (\d+)% reduce (\d+)%')

    for line in content:
        m = p.search(line)
        if m:
            dt_str = m.group(1)
            map_p = int(m.group(2))
            red_p = int(m.group(3))
            dt = datetime.datetime.strptime(dt_str, '%Y-%m-%d %H:%M:%S')

            if first_ts is None: first_ts = dt  # 以第一条进度日志作为 0秒

            elapsed = (dt - first_ts).total_seconds()
            progress_data.append({'Time_Sec': elapsed, 'Map': map_p, 'Reduce': red_p})

    return pd.DataFrame(progress_data)


# --- 4. 通用绘图函数 (复用您的风格代码) ---
def plot_beautiful_lines(data_dict, title, ylabel, output_filename, is_memory=True):
    plt.figure(figsize=(10, 6))  # 宽屏大气

    # 极简边框
    ax = plt.gca()
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_linewidth(1.2)
    ax.spines['bottom'].set_linewidth(1.2)

    colors = ["#4C72B0", "#DD8452", "#55A868", "#C44E52"]
    line_styles = ['-', '-', '-', '-']

    for i, val in enumerate(sorted(data_dict.keys())):
        df = data_dict[val]
        if df.empty: continue

        if is_memory:
            # 内存数据：计算均值
            raw_data = df.groupby('Time_Step')['MEM'].mean()
            x, y = raw_data.index.values, raw_data.values
        else:
            # 进度数据不需要计算均值，直接画 (这里预留接口，后面单独处理进度图)
            pass

        # 平滑处理
        if len(x) > 3:
            x_smooth = np.linspace(x.min(), x.max(), 300)
            spl = make_interp_spline(x, y, k=3)
            y_smooth = spl(x_smooth)
            y_smooth = np.clip(y_smooth, 0, 100)
        else:
            x_smooth, y_smooth = x, y

        # 绘图 + 阴影
        plt.plot(x_smooth, y_smooth,
                 label=f'SlowStart = {val}',
                 color=colors[i % len(colors)],
                 linestyle=line_styles[i % len(line_styles)],
                 linewidth=2.5, alpha=0.9)

        plt.fill_between(x_smooth, y_smooth, alpha=0.1, color=colors[i % len(colors)])

    plt.title(title, fontsize=16, fontweight='bold', pad=20)
    plt.xlabel('执行时间 (Time Step)', fontsize=13, labelpad=10)
    plt.ylabel(ylabel, fontsize=13, labelpad=10)
    plt.legend(frameon=False, fontsize=11, loc='best')
    plt.grid(True, linestyle=':', alpha=0.6, color='gray', zorder=0)
    plt.ylim(0, 105)

    plt.savefig(output_filename, bbox_inches='tight', dpi=300)
    plt.close()
    print(f"图表已生成: {output_filename}")


# --- 5. 单独绘制 Map/Reduce 进度对比图 (宽屏风格) ---
def plot_progress_overlap(prog_df, val, output_dir):
    if prog_df.empty: return

    plt.figure(figsize=(10, 6))
    ax = plt.gca()
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_linewidth(1.2)
    ax.spines['bottom'].set_linewidth(1.2)

    # 提取数据
    x = prog_df['Time_Sec'].values
    y_map = prog_df['Map'].values
    y_reduce = prog_df['Reduce'].values

    # 平滑处理 (为了美观，也做一点点平滑，但不能太厉害)
    if len(x) > 5:
        x_smooth = np.linspace(x.min(), x.max(), 300)
        try:
            spl_m = make_interp_spline(x, y_map, k=2)
            spl_r = make_interp_spline(x, y_reduce, k=2)
            y_map_s = np.clip(spl_m(x_smooth), 0, 100)
            y_reduce_s = np.clip(spl_r(x_smooth), 0, 100)
        except:
            x_smooth, y_map_s, y_reduce_s = x, y_map, y_reduce
    else:
        x_smooth, y_map_s, y_reduce_s = x, y_map, y_reduce

    # 绘制 Map 线 (蓝色)
    plt.plot(x_smooth, y_map_s, label='Map 进度', color='#4C72B0', linewidth=3)
    # 绘制 Reduce 线 (橙红色)
    plt.plot(x_smooth, y_reduce_s, label='Reduce 进度', color='#C44E52', linewidth=3, linestyle='--')

    # 关键：填充重叠区域 (Parallel Zone)
    # 使用灰色填充，展示两条线中间的差距
    plt.fill_between(x_smooth, y_map_s, y_reduce_s,
                     where=(y_map_s >= y_reduce_s),
                     color='gray', alpha=0.15, label='Map-Reduce 并行区')

    plt.title(f'任务执行进度重叠分析 (SlowStart = {val})', fontsize=16, fontweight='bold', pad=20)
    plt.xlabel('执行时间 (秒)', fontsize=13, labelpad=10)
    plt.ylabel('任务完成进度 (%)', fontsize=13, labelpad=10)

    plt.legend(frameon=False, fontsize=11, loc='lower right')
    plt.grid(True, linestyle=':', alpha=0.6, color='gray', zorder=0)
    plt.ylim(0, 105)

    save_path = os.path.join(output_dir, f'progress_overlap_{val}.png')
    plt.savefig(save_path, bbox_inches='tight', dpi=300)
    plt.close()
    print(f"进度图已生成: {save_path}")

# ==========================================================
# 一次性画 4 张进度重叠图（2×2 子图）
# ==========================================================
def plot_progress_grid(all_prog_data, output_dir):
    """
    all_prog_data: dict {slowstart: DataFrame['Time_Sec','Map','Reduce']}
    """
    fig, axes = plt.subplots(2, 2, figsize=(12, 7))   # 宽 12 高 7，留足横向空间
    axes = axes.flatten()                              # 按行拉平，方便循环
    colors = {'Map': '#4C72B0', 'Reduce': '#C44E52'}

    for idx, (val, df) in enumerate(sorted(all_prog_data.items())):
        ax = axes[idx]

        # ---- 平滑 ----
        x = df['Time_Sec'].values
        y_map = df['Map'].values
        y_red = df['Reduce'].values
        if len(x) > 5:
            x_s = np.linspace(x.min(), x.max(), 300)
            spl_m = make_interp_spline(x, y_map, k=2)
            spl_r = make_interp_spline(x, y_red, k=2)
            y_map_s = np.clip(spl_m(x_s), 0, 100)
            y_red_s = np.clip(spl_r(x_s), 0, 100)
        else:
            x_s, y_map_s, y_red_s = x, y_map, y_red

        # ---- 绘图 ----
        ax.plot(x_s, y_map_s, label='Map', color=colors['Map'], lw=2.5)
        ax.plot(x_s, y_red_s, label='Reduce', color=colors['Reduce'],
                lw=2.5, linestyle='--')
        ax.fill_between(x_s, y_map_s, y_red_s,
                        where=(y_map_s >= y_red_s),
                        color='gray', alpha=0.15)

        # ---- 标签 & 子图标题 ----
        ax.set_title(f'({chr(97+idx)}) SlowStart = {val}',
                     fontsize=13, fontweight='bold', pad=6)
        ax.set_ylim(0, 105)
        ax.grid(True, linestyle=':', alpha=0.5)
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_linewidth(1)
        ax.spines['bottom'].set_linewidth(1)

        # 只有底行显示 X 轴标签
        if idx >= 2:
            ax.set_xlabel('执行时间 (s)', fontsize=11)
        # 只有左列显示 Y 轴标签
        if idx % 2 == 0:
            ax.set_ylabel('任务进度 (%)', fontsize=11)

        # 图例只放在 (a) 里，其余子图不再重复
        if idx == 0:
            ax.legend(loc='lower right', frameon=False, fontsize=10)

    # ---- 整体标题（可选）----
    fig.suptitle('Map-Reduce 并行进度对比', fontsize=16, fontweight='bold', y=0.98)

    # ---- 微调布局 ----
    plt.tight_layout()
    save_path = os.path.join(output_dir, 'progress_overlap_grid.png')
    plt.savefig(save_path, bbox_inches='tight', dpi=300)
    plt.close()
    print(f'2×2 进度网格已生成：{save_path}')

# --- 主逻辑 ---
def main():
    base_dir = os.getcwd()
    experiment_dirs = [d for d in os.listdir(base_dir)
                       if os.path.isdir(d) and ('slowstart' in d.lower() or 'slow_start' in d.lower())]

    all_mem_data = {}
    output_dir = 'paper_figures_beautiful'
    os.makedirs(output_dir, exist_ok=True)

    print(f"开始分析... 输出目录: {output_dir}")

    for exp_dir in sorted(experiment_dirs):
        m = re.search(r'(\d+\.\d+|\d+)', exp_dir.split('slowstart')[-1])
        if not m: continue
        val = float(m.group(1))

        # 1. 收集内存数据
        mem_df = parse_monitor_mem(os.path.join(base_dir, exp_dir, 'monitor.log'))
        if not mem_df.empty:
            mem_df['Time_Step'] = mem_df['Time_Step'] - mem_df['Time_Step'].min()
            all_mem_data[val] = mem_df

        # 2. 直接绘制进度图 (每个参数画一张，方便挑选)
        # prog_df = parse_progress(os.path.join(base_dir, exp_dir, 'job_output.log'))
        # if not prog_df.empty:
        #     plot_progress_overlap(prog_df, val, output_dir)
        # 收集 4 个 slowstart 的进度 DataFrame
        # all_prog_data = {}
        # for exp_dir in sorted(experiment_dirs):
        #     m = re.search(r'(\d+\.\d+|\d+)', exp_dir.split('slowstart')[-1])
        #     if not m: continue
        #     val = float(m.group(1))
        #     prog_df = parse_progress(os.path.join(base_dir, exp_dir, 'job_output.log'))
        #     if not prog_df.empty:
        #         all_prog_data[val] = prog_df
        #
        # # 一次性画 2×2
        # plot_progress_grid(all_prog_data, output_dir)

    # 3. 汇总绘制内存对比图
    if all_mem_data:
        plot_beautiful_lines(
            all_mem_data,
            '不同策略下集群平均内存利用率趋势',
            '平均内存利用率 (%)',
            os.path.join(output_dir, 'cluster_memory_trend.png')
        )


if __name__ == "__main__":
    main()