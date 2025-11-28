import matplotlib.pyplot as plt
import numpy as np
import seaborn as sns
import os
from matplotlib import rcParams
from scipy.interpolate import make_interp_spline
from common_utils import scan_multiple_runs, average_monitor_data
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


def calculate_x_limit(max_times_sorted):
    """计算图表 X 轴限制"""
    if len(max_times_sorted) < 2:
        return int(max_times_sorted[-1]) if max_times_sorted else 100

    max_val = max_times_sorted[-1]
    second_max_val = max_times_sorted[-2]
    threshold = second_max_val * 1.1

    if max_val > threshold:
        x_limit = int(second_max_val)
        print(f"    [判断] 异常偏大，取第二高值: {x_limit}")
    else:
        x_limit = int(max_val)
        print(f"    [判断] 差异正常，保留最高值: {x_limit}")

    return x_limit


def plot_averaged_cpu_trends(averaged_monitor_data):
    """绘制平均化后的 CPU 趋势图"""
    output_dir = 'Averaged_CPU_Charts'
    os.makedirs(output_dir, exist_ok=True)

    colors = ["#4C72B0", "#DD8452", "#55A868", "#C44E52"]

    for ds_name, ss_dict in averaged_monitor_data.items():
        if not ss_dict:
            continue

        print(f"绘制平均 CPU 趋势图: {ds_name}")

        plt.figure(figsize=(12, 7))
        ax = plt.gca()
        ax.spines['top'].set_visible(False)
        ax.spines['right'].set_visible(False)
        ax.spines['left'].set_linewidth(1.2)
        ax.spines['bottom'].set_linewidth(1.2)

        sorted_ss = sorted(ss_dict.keys())

        # 计算 X 轴限制
        max_times = [df['Time_Step'].max() for df in ss_dict.values()]
        max_times_sorted = sorted(max_times)
        print(f"    所有线条的长度: {max_times_sorted}")
        x_limit = calculate_x_limit(max_times_sorted)

        # 绘制每条线
        for i, slowstart in enumerate(sorted_ss):
            df = ss_dict[slowstart]
            x = df['Time_Step'].values
            y = df['CPU'].values

            # 截断到 x_limit
            mask = x <= x_limit
            x, y = x[mask], y[mask]

            if len(x) == 0:
                print(f"      警告: SlowStart={slowstart} 无有效数据点")
                continue

            # 平滑处理
            if len(x) > 10:
                try:
                    x_smooth = np.linspace(x.min(), x.max(), 300)
                    spl = make_interp_spline(x, y, k=3)
                    y_smooth = spl(x_smooth)
                    y_smooth = np.clip(y_smooth, 0, 100)
                except Exception as e:
                    print(f"      平滑处理失败: {e}，使用原始数据")
                    x_smooth, y_smooth = x, y
            else:
                x_smooth, y_smooth = x, y

            plt.plot(x_smooth, y_smooth,
                     label=f'SlowStart = {slowstart}',
                     color=colors[i % len(colors)],
                     linewidth=2.8,
                     alpha=0.9)

            plt.fill_between(x_smooth, y_smooth, alpha=0.12, color=colors[i % len(colors)])

        plt.title(f'数据集 {ds_name} 的集群 CPU 负载特征 (三次实验平均)',
                  fontsize=17, fontweight='bold', pad=20)
        plt.xlabel('执行时间 (s)', fontsize=14, labelpad=10)
        plt.ylabel('集群平均 CPU 利用率 (%)', fontsize=14, labelpad=10)
        plt.legend(frameon=False, fontsize=12, loc='best')
        plt.grid(True, linestyle=':', alpha=0.6, color='gray', zorder=0)
        plt.ylim(0, 105)
        plt.xlim(0, x_limit)

        plt.tight_layout()

        save_path = os.path.join(output_dir, f'Averaged_CPU_Trend_{ds_name}.png')
        plt.savefig(save_path, bbox_inches='tight', dpi=300, facecolor='white')
        plt.close()

        print(f"  已保存: {save_path}")

    print(f"\n所有 CPU 趋势图已生成在: {output_dir}/")


def main():
    base_dir = './MapReduceLog'

    print("=" * 60)
    print("CPU 趋势图生成器")
    print("=" * 60)

    print("\n扫描多轮实验数据...")
    monitor_data, _ = scan_multiple_runs(base_dir)

    if not monitor_data:
        print("未找到 monitor 数据")
        print("请检查目录结构")

    print("\n计算 Monitor 数据平均值...")
    averaged_monitor = average_monitor_data(monitor_data)

    if not averaged_monitor:
        print("错误：平均化后无可用数据")
        return

    print("\n生成平均 CPU 趋势图...")
    plot_averaged_cpu_trends(averaged_monitor)

    print("\nCPU 图表生成完成！")


if __name__ == "__main__":
    main()
