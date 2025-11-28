import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import os
import re
from matplotlib import rcParams
from scipy.interpolate import make_interp_spline

# 全局样式配置
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


# 解析 monitor.log
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

        # 匹配 [node] CPU: X% | MEM: Y%
        match = re.match(r'.*\[(\w+-\w+)\] CPU: (\d+\.\d+)% \| MEM: (\d+)%', line)

        if not match:
            match = re.match(r'\[(\w+-\w+)\] CPU: (\d+\.\d+)% \| MEM: (\d+)%', line)

        if match:
            if current_time_step == -1 and '----' not in "".join(lines[:20]):
                current_time_step = len(data_by_time) // 3

            cpu_usage = float(match.group(2))
            mem_usage = int(match.group(3))

            step_val = max(0, current_time_step)

            data_by_time.append({
                'Time_Step': step_val,
                'Node': match.group(1),
                'CPU': cpu_usage,
                'MEM': mem_usage
            })

    if not data_by_time:
        return pd.DataFrame()
    return pd.DataFrame(data_by_time)


# 计算 X 轴限制
def calculate_x_limit(max_times_sorted):
    """
    修改的取值逻辑：
    如果最高值 > 第二高值 * 1.1 (即大于 10%)，则取第二高值
    否则取最高值
    """
    if len(max_times_sorted) < 2:
        return int(max_times_sorted[-1]) if max_times_sorted else 100

    max_val = max_times_sorted[-1]  # 最高值
    second_max_val = max_times_sorted[-2]  # 第二高值

    # 判断条件：最高值是否大于第二高值的 10%
    threshold = second_max_val * 1.1

    if max_val > threshold:
        # 最高值异常偏大，取第二高值
        x_limit = int(second_max_val)
        print(f"    [判断] 最高值 {max_val} > 第二高值 {second_max_val} × 1.1 ({threshold:.0f})")
        print(f"    [结论] 异常偏大，取第二高值: {x_limit}")
    else:
        # 差异在可接受范围内，保留最高值
        x_limit = int(max_val)
        print(f"    [判断] 最高值 {max_val} ≤ 第二高值 {second_max_val} × 1.1 ({threshold:.0f})")
        print(f"    [结论] 差异在范围内，保留最高值: {x_limit}")

    return x_limit


# 绘制内存趋势图
def plot_mem_trend(ds_name, ss_dict, output_dir):
    if not ss_dict:
        return

    print(f"正在绘制内存趋势: {ds_name} ...")

    plt.figure(figsize=(10, 6))
    ax = plt.gca()
    ax.spines['top'].set_visible(False)
    ax.spines['right'].set_visible(False)
    ax.spines['left'].set_linewidth(1.2)
    ax.spines['bottom'].set_linewidth(1.2)

    sorted_ss = sorted(ss_dict.keys())
    colors = ["#4C72B0", "#DD8452", "#55A868", "#C44E52"]

    # 计算所有曲线的最大时间
    max_times = []
    for val in sorted_ss:
        m_df = ss_dict[val]
        max_time = m_df['Time_Step'].max()
        max_times.append(max_time)
        print(f"    {ds_name} - SlowStart={val}: 最大 Time Step = {max_time}")

    # 判断异常值
    max_times_sorted = sorted(max_times)
    print(f"    所有线条的长度: {max_times_sorted}")

    x_limit = calculate_x_limit(max_times_sorted)

    # 逐条线绘制
    for i, val in enumerate(sorted_ss):
        m_df = ss_dict[val]

        # 提取 MEM 数据
        raw_data = m_df.groupby('Time_Step')['MEM'].mean()
        x = raw_data.index.values
        y = raw_data.values

        # 只保留 x <= x_limit 的数据
        mask = x <= x_limit
        x = x[mask]
        y = y[mask]

        if len(x) == 0:
            continue

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

    plt.title(f'数据集 {ds_name} 下的集群内存利用率趋势', fontsize=16, fontweight='bold', pad=20)
    plt.xlabel('执行时间 (Time Step)', fontsize=13, labelpad=10)
    plt.ylabel('集群平均内存利用率 (%)', fontsize=13, labelpad=10)
    plt.legend(frameon=False, fontsize=11, loc='best')
    plt.grid(True, linestyle=':', alpha=0.6, color='gray', zorder=0)
    plt.ylim(0, 105)
    plt.xlim(0, x_limit)

    save_path = os.path.join(output_dir, f'MEM_Trend_{ds_name}.png')
    plt.savefig(save_path, bbox_inches='tight', dpi=300)
    plt.close()

    print(f"  ✓ 内存图已生成: {save_path}\n")


# 主函数
def main():
    base_dir = './MapReduceLog'

    if not os.path.exists(base_dir):
        print(f"错误：找不到目录 {base_dir}")
        return

    all_data = {}

    print("正在扫描数据...")
    for folder_name in os.listdir(base_dir):
        folder_path = os.path.join(base_dir, folder_name)
        if not os.path.isdir(folder_path):
            continue

        # 提取数据集名称和 slowstart
        match = re.search(r'(\d+(?:G|M|MB|GB)?)_slowstart_([\d\.]+)', folder_name, re.IGNORECASE)

        if match:
            ds = match.group(1)
            ss = float(match.group(2))

            if ds not in all_data:
                all_data[ds] = {}

            log_path = os.path.join(folder_path, 'monitor.log')
            df = parse_monitor_log(log_path)

            if not df.empty:
                min_time = df['Time_Step'].min()
                df['Time_Step'] = df['Time_Step'] - min_time
                all_data[ds][ss] = df
                print(f"  [+] 读取成功: {ds} - SS:{ss}")

    # 准备输出
    output_dir = 'Final_Output_Charts'
    os.makedirs(output_dir, exist_ok=True)

    # 逐数据集绘制内存趋势图
    for ds_name, ss_dict in all_data.items():
        if ss_dict:
            plot_mem_trend(ds_name, ss_dict, output_dir)

    print(f"所有图表已生成在: {output_dir}")


if __name__ == "__main__":
    main()