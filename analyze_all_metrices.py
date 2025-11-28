import pandas as pd
import os
import re
import datetime
import numpy as np

# 配置：pandas 显示选项
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.precision', 1)  # 保留1位小数


def parse_logs(base_dir):
    results = []

    if not os.path.exists(base_dir):
        print(f"错误：目录 {base_dir} 不存在")
        return pd.DataFrame()

    for folder_name in os.listdir(base_dir):
        folder_path = os.path.join(base_dir, folder_name)
        if not os.path.isdir(folder_path):
            continue

        # 1. 提取数据集名称和 Slowstart 值
        match = re.search(r'(\d+(?:G|M|MB|GB)?)_slowstart_([\d\.]+)', folder_name, re.IGNORECASE)
        if not match:
            continue

        ds_name = match.group(1)
        ss_val = float(match.group(2))

        # 2. 解析 job_output.log
        job_log = os.path.join(folder_path, 'job_output.log')
        duration = np.nan
        gc_time = np.nan

        if os.path.exists(job_log):
            with open(job_log, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()
                timestamps = re.findall(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})', content)
                if len(timestamps) >= 2:
                    start = datetime.datetime.strptime(timestamps[0], '%Y-%m-%d %H:%M:%S')
                    end = datetime.datetime.strptime(timestamps[-1], '%Y-%m-%d %H:%M:%S')
                    duration = (end - start).total_seconds()

                gc_match = re.search(r'GC time elapsed \(ms\)=(\d+)', content)
                if gc_match:
                    gc_time = int(gc_match.group(1)) / 1000.0

        # 3. 解析 monitor.log
        mon_log = os.path.join(folder_path, 'monitor.log')
        avg_cpu = np.nan

        if os.path.exists(mon_log):
            cpu_vals = []
            with open(mon_log, 'r', encoding='utf-8', errors='ignore') as f:
                for line in f:
                    m = re.search(r'CPU: (\d+\.\d+)%', line)
                    if m:
                        cpu_vals.append(float(m.group(1)))
            if cpu_vals:
                avg_cpu = np.mean(cpu_vals)

        results.append({
            'Dataset': ds_name,
            'SlowStart': ss_val,
            'Total_Time(s)': duration,
            'Avg_CPU(%)': avg_cpu,
            'GC_Time(s)': gc_time
        })

    return pd.DataFrame(results)


def process_and_save(df, value_col, filename, criteria="min"):
    """
    生成透视表，打印到控制台，并保存为 CSV
    """
    if df.empty: return

    # 生成透视表
    pivot = df.pivot_table(index='Dataset', columns='SlowStart', values=value_col, aggfunc='mean')

    # 排序：列按 slowstart 大小，行按数据集大小
    pivot = pivot.reindex(sorted(pivot.columns), axis=1)

    def sort_key(name):
        num = re.search(r'\d+', name)
        val = int(num.group()) if num else 0
        if 'G' in name.upper(): val *= 1000
        return val

    sorted_idx = sorted(pivot.index, key=sort_key)
    pivot = pivot.reindex(sorted_idx)

    # --- 1. 保存 CSV ---
    # 保存前，我们添加一列 "Best_SS" 到 CSV 中方便查看
    pivot_csv = pivot.copy()
    best_ss_col = []
    for idx, row in pivot_csv.iterrows():
        if criteria == 'min':
            best_val = row.min()
        else:
            best_val = row.max()
        # 找到最优的列名
        best_cols = row[row == best_val].index.tolist()
        best_ss_col.append(",".join(map(str, best_cols)))

    pivot_csv['Best_SlowStart'] = best_ss_col
    pivot_csv.to_csv(filename)
    print(f"[已保存] {filename}")

    # --- 2. 打印控制台 (保持原有美观格式) ---
    title_map = {
        'Total_Time(s)': '任务总耗时',
        'Avg_CPU(%)': '集群平均 CPU 利用率',
        'GC_Time(s)': 'GC 总耗时'
    }
    title = title_map.get(value_col, value_col)

    print(f"\n【 指标：{title} 】 ({'数值越小越优' if criteria == 'min' else '数值越大越优'})")
    print("-" * 65)
    header = f"{'Dataset':<10} | " + " | ".join([f"SS={str(c):<4}" for c in pivot.columns]) + " | Best SS"
    print(header)
    print("-" * len(header))

    for idx, row in pivot.iterrows():
        if criteria == 'min':
            best_val = row.min()
        else:
            best_val = row.max()

        line_str = f"{idx:<10} | "
        best_ss_list = []

        for col in pivot.columns:
            val = row[col]
            if pd.isna(val):
                line_str += f"{'N/A':<7} | "
                continue

            if val == best_val:
                best_ss_list.append(str(col))
                line_str += f"*{val:<6.1f} | "
            else:
                line_str += f"{val:<7.1f} | "

        print(f"{line_str} -> {','.join(best_ss_list)}")
    print("-" * 65)


def main():
    base_dir = './MapReduceLog'
    df = parse_logs(base_dir)

    if df.empty:
        print("未找到数据，请检查路径。")
        return

    print("=" * 65)
    print("  MapReduce SlowStart 性能分析 (生成CSV版)")
    print("=" * 65)

    # 1. 保存原始明细数据
    df.to_csv("result_raw.csv", index=False)
    print("[已保存] result_raw.csv (原始数据)")

    # 2. 生成各个指标的透视表并保存
    process_and_save(df, 'Total_Time(s)', 'result_time.csv', criteria='min')
    process_and_save(df, 'Avg_CPU(%)', 'result_cpu.csv', criteria='max')
    process_and_save(df, 'GC_Time(s)', 'result_gc.csv', criteria='min')

    print("\n所有分析完成！CSV 文件已保存在当前目录下。")


if __name__ == "__main__":
    main()