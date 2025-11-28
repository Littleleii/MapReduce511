import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import os
import re
from matplotlib import rcParams
from scipy.interpolate import make_interp_spline

# --- 1. 全局样式配置 (保持原样) ---
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


# --- 2. 解析 monitor.log (完全保留你提供的原始逻辑) ---
# 唯一的微调：增加了编码容错，防止读取报错
def parse_monitor_log(log_path):
    if not os.path.exists(log_path):
        return pd.DataFrame()

    with open(log_path, 'r', encoding='utf-8', errors='ignore') as f:
        lines = f.readlines()

    data_by_time = []
    current_time_step = -1

    # 你的原始逻辑：依赖 ---- 分隔符或行数
    # 如果日志里没有 ----，这个逻辑可能会把所有数据堆在 time_step=-1
    # 但既然你之前能跑通，说明你的日志里肯定有这个结构，或者有其他兼容方式
    for line in lines:
        if 'Real Performance Monitor Started' in line or not line.strip():
            continue

        # 原始逻辑：遇到分隔符时间步+1
        if '----' in line:
            current_time_step += 1
            continue

        # 原始正则：匹配 [node] CPU: ...
        match = re.match(r'.*\[(\w+-\w+)\] CPU: (\d+\.\d+)% \| MEM: (\d+)%', line)

        # 如果上面的匹配失败，尝试匹配不带前面内容的（兼容性）
        if not match:
            match = re.match(r'\[(\w+-\w+)\] CPU: (\d+\.\d+)% \| MEM: (\d+)%', line)

        if match:
            # 如果从来没遇到过 ----，我们手动让时间步递增，防止所有数据重叠
            # (这是为了防止新日志格式没有----导致画不出图的保险措施)
            if current_time_step == -1 and '----' not in "".join(lines[:20]):
                # 简单粗暴：每行算一个时间点（仅在没有分隔符时生效）
                current_time_step = len(data_by_time) // 3

            cpu_usage = float(match.group(2))
            mem_usage = int(match.group(3))

            # 修正：确保 time_step 至少为 0
            step_val = max(0, current_time_step)

            data_by_time.append(
                {'Time_Step': step_val, 'Node': match.group(1), 'CPU': cpu_usage, 'MEM': mem_usage})

    if not data_by_time:
        return pd.DataFrame()
    return pd.DataFrame(data_by_time)


# --- 3. 主函数 (修改为支持多数据集) ---
def main():
    # 1. 指定你的日志根目录
    # 注意：这里我改成了 ./MapReduceLog，因为你之前的截图显示是在这个目录下
    # 如果你的脚本就在 MapReduceLog 同级，这样写是对的
    base_dir = './MapReduceLog'

    if not os.path.exists(base_dir):
        print(f"错误：找不到目录 {base_dir}")
        return

    # 2. 扫描所有文件夹并分类
    # 结构: all_data['1G']['0.2'] = dataframe
    all_data = {}

    print("正在扫描数据...")
    for folder_name in os.listdir(base_dir):
        folder_path = os.path.join(base_dir, folder_name)
        if not os.path.isdir(folder_path):
            continue

        # 宽松匹配：找 1G/5G/100mb 和 slowstart数字
        # 这里的正则比你原来的更通用一点，能匹配 _1G_slowstart_0.2
        match_ds = re.search(r'(1G|5G|100mb)', folder_name, re.IGNORECASE)
        match_ss = re.search(r'slowstart.*?([\d\.]+)', folder_name, re.IGNORECASE)

        if match_ds and match_ss:
            ds = match_ds.group(1)
            ss = float(match_ss.group(1))

            if ds not in all_data: all_data[ds] = {}

            log_path = os.path.join(folder_path, 'monitor.log')
            df = parse_monitor_log(log_path)

            if not df.empty:
                # 数据清洗：让时间从 0 开始
                min_time = df['Time_Step'].min()
                df['Time_Step'] = df['Time_Step'] - min_time
                all_data[ds][ss] = df
                print(f"  [+] 读取成功: {ds} - SS:{ss}")

    # 3. 准备输出
    output_dir = 'Final_Output_Charts'
    os.makedirs(output_dir, exist_ok=True)

    colors = ["#4C72B0", "#DD8452", "#55A868", "#C44E52"]
    line_styles = ['-', '-', '-', '-']  # 全实线

    # 4. 循环绘图 (每种数据集一张图)
    for ds_name, ss_dict in all_data.items():
        if not ss_dict: continue

        print(f"正在绘制数据集: {ds_name} ...")

        plt.figure(figsize=(10, 6))
        ax = plt.gca()
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_linewidth(1.2)
        ax.spines['bottom'].set_linewidth(1.2)

        sorted_ss = sorted(ss_dict.keys())

        # --- 计算所有曲线的最大时间 ---
        max_times = []
        for val in sorted_ss:
            m_df = ss_dict[val]
            max_time = m_df['Time_Step'].max()
            max_times.append(max_time)
            print(f"    {ds_name} - SlowStart={val}: 最大 Time Step = {max_time}")

        # ★★★ 改进：去掉最长的，取第二长的 ★★★
        max_times_sorted = sorted(max_times)
        if len(max_times_sorted) >= 2:
            x_limit = int(max_times_sorted[-2])  # 倒数第二个（第二长）
            print(f"    所有线条的长度: {max_times_sorted}")
            print(f"    [设置] X 轴限制为: {x_limit} (去掉最长的 {max_times_sorted[-1]})")
        else:
            x_limit = int(max_times_sorted[-1])  # 如果只有1条线，就用那条

        for i, val in enumerate(sorted_ss):
            m_df = ss_dict[val]

            # 计算原始数据点
            raw_data = m_df.groupby('Time_Step')['CPU'].mean()
            x = raw_data.index.values
            y = raw_data.values

            # ★★★ 核心修复：只保留 x <= x_limit 的数据 ★★★
            mask = x <= x_limit
            x = x[mask]
            y = y[mask]

            # 平滑处理
            if len(x) > 10:
                try:
                    x_smooth = np.linspace(x.min(), x.max(), 300)
                    spl = make_interp_spline(x, y, k=3)
                    y_smooth = spl(x_smooth)
                    y_smooth = np.clip(y_smooth, 0, 100)
                except:
                    x_smooth, y_smooth = x, y
            else:
                x_smooth, y_smooth = x, y

            plt.plot(x_smooth, y_smooth,
                     label=f'SlowStart = {val}',
                     color=colors[i % 4],
                     linestyle='-',
                     linewidth=2.5,
                     alpha=0.9)

            plt.fill_between(x_smooth, y_smooth, alpha=0.1, color=colors[i % 4])

        plt.title(f'数据集 {ds_name} 下的集群 CPU 负载特征', fontsize=16, fontweight='bold', pad=20)
        plt.xlabel('执行时间 (Time Step)', fontsize=13, labelpad=10)
        plt.ylabel('集群平均 CPU 利用率 (%)', fontsize=13, labelpad=10)
        plt.legend(frameon=False, fontsize=11, loc='best')
        plt.grid(True, linestyle=':', alpha=0.6, color='gray', zorder=0)
        plt.ylim(0, 105)

        # ★★★ 核心修复：设置 X 轴范围 ★★★
        plt.xlim(0, x_limit)

        save_path = os.path.join(output_dir, f'CPU_Trend_{ds_name}.png')
        plt.savefig(save_path, bbox_inches='tight', dpi=300)
        plt.close()

    print(f"所有图表已生成在: {output_dir}")


if __name__ == "__main__":
    main()