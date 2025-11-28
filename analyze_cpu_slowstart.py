import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import os
import re
import datetime
from matplotlib import rcParams
from scipy.interpolate import make_interp_spline  # 引入平滑插值库

# --- 1. 全局样式配置 (论文级审美) ---
config = {
    "font.family": 'serif',
    "font.size": 12,
    "mathtext.fontset": 'stix',
    "font.serif": ['SimHei'],
}
rcParams.update(config)
plt.rcParams['axes.unicode_minus'] = False
# 使用 Seaborn 的 "paper" 上下文，字体稍微缩小，适合文档
sns.set_context("paper", font_scale=1.2)
sns.set_style("whitegrid", {"font.sans-serif": ['SimHei', 'Microsoft YaHei']})
plt.rcParams['figure.dpi'] = 300


# --- 2. 解析 monitor.log (保持不变) ---
def parse_monitor_log(log_path):
    if not os.path.exists(log_path):
        return pd.DataFrame()

    with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
        lines = f.readlines()

    data_by_time = []
    current_time_step = -1
    for line in lines:
        if 'Real Performance Monitor Started' in line or not line.strip():
            continue
        if '----' in line:
            current_time_step += 1
            continue
        match = re.match(r'\[(\w+-\w+)\] CPU: (\d+\.\d+)% \| MEM: (\d+)%', line)
        if match:
            cpu_usage = float(match.group(2))
            mem_usage = int(match.group(3))
            data_by_time.append(
                {'Time_Step': current_time_step, 'Node': match.group(1), 'CPU': cpu_usage, 'MEM': mem_usage})

    if not data_by_time:
        return pd.DataFrame()
    return pd.DataFrame(data_by_time)


# --- 3. 解析 job_output.log (保持不变) ---
def parse_job_output_log(log_path):
    job_info = {}
    if not os.path.exists(log_path):
        return job_info
    try:
        with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()
        # ... (省略原有解析代码，保持您的原始逻辑不变) ...
        # 这里为了节省篇幅省略了，请保留您原有的解析逻辑
        pass
    except Exception:
        pass
    return job_info


# --- 4. 主函数 (核心修改部分) ---
def main():
    base_dir = os.getcwd()
    experiment_dirs = [d for d in os.listdir(base_dir)
                       if os.path.isdir(d) and ('slowstart' in d.lower() or 'slow_start' in d.lower())]

    all_monitor_data = {}

    # 1. 数据收集
    for exp_dir in sorted(experiment_dirs):
        m = re.search(r'(\d+\.\d+|\d+)', exp_dir.split('slowstart')[-1])
        if not m: continue
        slowstart_val = float(m.group(1))

        monitor_path = os.path.join(base_dir, exp_dir, 'monitor.log')
        monitor_df = parse_monitor_log(monitor_path)

        if not monitor_df.empty:
            # 数据清洗：让时间从 0 开始
            min_time = monitor_df['Time_Step'].min()
            monitor_df['Time_Step'] = monitor_df['Time_Step'] - min_time
            all_monitor_data[slowstart_val] = monitor_df

    output_dir = 'paper_figures_final'
    os.makedirs(output_dir, exist_ok=True)

    print("正在生成高颜值曲线图...")

    # ==========================================================
    # 高级绘图：平滑曲线 + 学术配色
    # ==========================================================

    # 设置画布大小，稍微宽一点更显大气
    plt.figure(figsize=(10, 6))

    # 移除顶部和右侧的边框 (Spines)
    ax = plt.gca()
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    # 加粗左侧和底部的坐标轴线
    ax.spines['left'].set_linewidth(1.2)
    ax.spines['bottom'].set_linewidth(1.2)

    # 定义高级配色 (来源于 Seaborn Deep 或 Tableau)
    # 对应：蓝色(0.2), 橙色(0.5), 绿色(0.8), 红色(1.0)
    colors = ["#4C72B0", "#DD8452", "#55A868", "#C44E52"]
    line_styles = ['-', '-', '-', '-']  # 也可以用 ['-', '--', '-.', ':'] 来区分

    # 遍历数据
    for i, val in enumerate(sorted(all_monitor_data.keys())):
        m_df = all_monitor_data[val]
        if m_df.empty: continue

        # 计算原始数据点
        raw_data = m_df.groupby('Time_Step')['CPU'].mean()
        x = raw_data.index.values
        y = raw_data.values

        # --- 核心技巧：曲线平滑 (Spline Interpolation) ---
        if len(x) > 3:  # 点太少没法平滑
            # 创建更密集的 X 轴 (比如从原来的10个点变成300个点)
            x_smooth = np.linspace(x.min(), x.max(), 300)
            # 创建插值函数 (k=3 代表三次样条插值，最平滑)
            spl = make_interp_spline(x, y, k=3)
            y_smooth = spl(x_smooth)

            # 修正：平滑后可能会稍微超出 0-100 范围，需要截断
            y_smooth = np.clip(y_smooth, 0, 100)
        else:
            x_smooth, y_smooth = x, y

        # 绘制曲线
        plt.plot(x_smooth, y_smooth,
                 label=f'SlowStart = {val}',
                 color=colors[i % len(colors)],
                 linestyle=line_styles[i % len(line_styles)],
                 linewidth=2.5,  # 线条稍微加粗
                 alpha=0.9)  # 稍微透明一点点

        # (可选) 添加一点点阴影，让图更有质感，如果不喜欢可以注释掉下面这行
        plt.fill_between(x_smooth, y_smooth, alpha=0.1, color=colors[i % len(colors)])

    # 标题和标签
    plt.title('不同启动策略下的集群 CPU 负载特征', fontsize=16, fontweight='bold', pad=20)
    plt.xlabel('执行时间 (Time Step)', fontsize=13, labelpad=10)
    plt.ylabel('集群平均 CPU 利用率 (%)', fontsize=13, labelpad=10)

    # 优化图例：去掉边框，放在合适位置
    plt.legend(frameon=False, fontsize=11, loc='best')

    # 优化网格：用虚线，颜色淡一点，置于底层
    plt.grid(True, linestyle=':', alpha=0.6, color='gray', zorder=0)

    # Y轴范围固定
    plt.ylim(0, 105)

    # 保存图片
    save_path = os.path.join(output_dir, 'cluster_cpu_trend_beautiful.png')
    plt.savefig(save_path, bbox_inches='tight', dpi=300)  # 高清保存
    plt.close()

    print(f"高颜值图表已生成: {save_path}")


if __name__ == "__main__":
    main()