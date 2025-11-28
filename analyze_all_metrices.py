import pandas as pd
import os
import re
import numpy as np
from common_utils import scan_multiple_runs, average_stage_data, sort_dataset_key

# 配置：pandas 显示选项
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)
pd.set_option('display.precision', 2)  # ← 改为保留2位小数


def build_dataframe_from_averaged_data(monitor_data, stage_data):
    """从平均化后的数据构建 DataFrame（数值保留2位小数）"""
    results = []
    all_datasets = set(monitor_data.keys()) | set(stage_data.keys())

    for dataset in all_datasets:
        ss_from_monitor = set(monitor_data.get(dataset, {}).keys())
        ss_from_stage = set(stage_data.get(dataset, {}).keys())
        all_slowstarts = ss_from_monitor | ss_from_stage

        for slowstart in all_slowstarts:
            record = {
                'Dataset': dataset,
                'SlowStart': slowstart,
                'Total_Time(s)': np.nan,
                'Avg_CPU(%)': np.nan,
                'Map_Time(s)': np.nan,
                'Shuffle_Time(s)': np.nan,
                'Reduce_Time(s)': np.nan,
                'Overlap_Ratio(%)': np.nan
            }

            # 从 stage_data 获取时间信息（保留2位小数）
            if dataset in stage_data and slowstart in stage_data[dataset]:
                stage_info = stage_data[dataset][slowstart]
                record['Total_Time(s)'] = round(stage_info.get('总耗时(s)', np.nan), 2)  # ← 添加 round
                record['Map_Time(s)'] = round(stage_info.get('Map耗时(s)', np.nan), 2)
                record['Shuffle_Time(s)'] = round(stage_info.get('Shuffle耗时(s)', np.nan), 2)
                record['Reduce_Time(s)'] = round(stage_info.get('Reduce耗时(s)', np.nan), 2)
                record['Overlap_Ratio(%)'] = round(stage_info.get('Shuffle重叠比(%)', np.nan), 2)

            # 从 monitor_data 获取 CPU 信息（保留2位小数）
            if dataset in monitor_data and slowstart in monitor_data[dataset]:
                df = monitor_data[dataset][slowstart]
                avg_cpu = round(df['CPU'].mean(), 2)  # ← 添加 round
                record['Avg_CPU(%)'] = avg_cpu

            results.append(record)

    return pd.DataFrame(results)


def process_and_save(df, value_col, filename, criteria="min", output_dir='Analysis_Results'):
    """生成透视表并保存（数值保留2位小数）"""
    if df.empty:
        return

    os.makedirs(output_dir, exist_ok=True)

    # 生成透视表（保留2位小数）
    pivot = df.pivot_table(index='Dataset', columns='SlowStart', values=value_col, aggfunc=lambda x: round(np.mean(x), 2))  # ← 添加 round

    # 排序
    pivot = pivot.reindex(sorted(pivot.columns), axis=1)
    sorted_idx = sorted(pivot.index, key=sort_dataset_key)
    pivot = pivot.reindex(sorted_idx)

    # --- 保存 CSV ---
    pivot_csv = pivot.copy()
    best_ss_col = []
    for idx, row in pivot_csv.iterrows():
        valid_values = row.dropna()
        if len(valid_values) == 0:
            best_ss_col.append("N/A")
            continue

        if criteria == 'min':
            best_val = valid_values.min()
        else:
            best_val = valid_values.max()

        best_cols = valid_values[valid_values == best_val].index.tolist()
        best_ss_col.append(",".join(map(str, best_cols)))

    pivot_csv['Best_SlowStart'] = best_ss_col
    full_path = os.path.join(output_dir, filename)
    pivot_csv.to_csv(full_path, float_format='%.2f')  # ← 确保CSV保留2位小数
    print(f"  ✓ 已保存: {full_path}")

    # --- 打印控制台 ---
    title_map = {
        'Total_Time(s)': '任务总耗时',
        'Avg_CPU(%)': '集群平均 CPU 利用率',
        'Map_Time(s)': 'Map 阶段耗时',
        'Shuffle_Time(s)': 'Shuffle 阶段耗时',
        'Reduce_Time(s)': 'Reduce 阶段耗时',
        'Overlap_Ratio(%)': 'Shuffle 重叠比例'
    }
    title = title_map.get(value_col, value_col)

    print(f"\n【 指标：{title} (三次实验平均) 】 ({'数值越小越优' if criteria == 'min' else '数值越大越优'})")
    print("-" * 80)
    header = f"{'Dataset':<10} | " + " | ".join([f"SS={str(c):<4}" for c in pivot.columns]) + " | Best SS"
    print(header)
    print("-" * 80)

    for idx, row in pivot.iterrows():
        valid_values = row.dropna()
        if len(valid_values) == 0:
            continue

        if criteria == 'min':
            best_val = valid_values.min()
        else:
            best_val = valid_values.max()

        line_str = f"{idx:<10} | "
        best_ss_list = []

        for col in pivot.columns:
            val = row[col]
            if pd.isna(val):
                line_str += f"{'N/A':<7} | "
                continue

            if val == best_val:
                best_ss_list.append(str(col))
                line_str += f"*{val:<6.2f} | "  # ← 改为 .2f
            else:
                line_str += f"{val:<7.2f} | "  # ← 改为 .2f

        print(f"{line_str} -> {','.join(best_ss_list)}")
    print("-" * 80)


def main():
    base_dir = './MapReduceLog'
    output_dir = 'Analysis_Results'

    print("=" * 80)
    print("  MapReduce SlowStart 性能分析 (基于三次实验平均)")
    print("=" * 80)

    print("\n1. 扫描多轮实验数据...")
    monitor_data, stage_data = scan_multiple_runs(base_dir)

    if not monitor_data and not stage_data:
        print("❌ 未找到数据，请检查路径")
        return

    print("\n2. 计算平均值...")
    from common_utils import average_monitor_data
    averaged_monitor = average_monitor_data(monitor_data)
    averaged_stage = average_stage_data(stage_data)

    print("\n3. 构建分析表格...")
    df = build_dataframe_from_averaged_data(averaged_monitor, averaged_stage)

    if df.empty:
        print("❌ 未能生成分析数据")
        return

    os.makedirs(output_dir, exist_ok=True)
    raw_file = os.path.join(output_dir, "result_raw.csv")
    df.to_csv(raw_file, index=False, float_format='%.2f')  # ← CSV保留2位小数
    print(f"\n  ✓ 已保存原始数据: {raw_file}")

    print("\n4. 生成各指标分析表...")
    process_and_save(df, 'Total_Time(s)', 'result_time.csv', criteria='min', output_dir=output_dir)
    process_and_save(df, 'Avg_CPU(%)', 'result_cpu.csv', criteria='max', output_dir=output_dir)
    process_and_save(df, 'Map_Time(s)', 'result_map.csv', criteria='min', output_dir=output_dir)
    process_and_save(df, 'Shuffle_Time(s)', 'result_shuffle.csv', criteria='min', output_dir=output_dir)
    process_and_save(df, 'Reduce_Time(s)', 'result_reduce.csv', criteria='min', output_dir=output_dir)
    process_and_save(df, 'Overlap_Ratio(%)', 'result_overlap.csv', criteria='max', output_dir=output_dir)

    print(f"\n✅ 所有分析完成！CSV 文件已保存在 {output_dir}/ 目录下")


if __name__ == "__main__":
    main()