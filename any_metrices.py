# analyze_hadoop_tasks_batch_v4.py
# 批量分析 Hadoop 任务日志
# 新增功能：同时分析网络 I/O (Shuffle) 和 磁盘 I/O (Spill)

import re
import pandas as pd
import os
from datetime import datetime


# ===========================
# 工具函数
# ===========================
def log(message, file_handle=None):
    print(message)
    if file_handle:
        file_handle.write(message + "\n")


# ===========================
# 数据解析逻辑
# ===========================
def extract_data_from_file(file_path):
    try:
        with open(file_path, 'r', encoding='utf-8') as f:
            content = f.read()
    except:
        try:
            with open(file_path, 'r', encoding='gbk') as f:
                content = f.read()
        except:
            with open(file_path, 'r', encoding='latin1') as f:
                content = f.read()

    pattern = r'var tasksTableData=\s*\[\s*(.+)\s*\]'
    match = re.search(pattern, content, re.DOTALL)

    if not match:
        return None

    data_block = match.group(1)
    row_pattern = r'\[([^\[\]]+)\]'
    rows = re.findall(row_pattern, data_block)

    parsed_data = []
    for row in rows:
        field_pattern = r'""([^"]*?)""'
        fields = re.findall(field_pattern, row)
        if fields:
            cleaned_fields = [re.sub(r'<[^>]+>', '', f) for f in fields]
            parsed_data.append(cleaned_fields)

    return parsed_data


def parse_map_tasks(file_path):
    data = extract_data_from_file(file_path)
    if not data: return pd.DataFrame()
    df = pd.DataFrame(data, columns=[
        'TaskName', 'State', 'StartTime', 'FinishTime', 'ElapsedTime',
        'AttemptStartTime', 'AttemptFinishTime', 'AttemptElapsedTime'
    ])
    for col in ['StartTime', 'FinishTime', 'ElapsedTime', 'AttemptStartTime', 'AttemptFinishTime',
                'AttemptElapsedTime']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    return df


def parse_reduce_tasks(file_path):
    data = extract_data_from_file(file_path)
    if not data: return pd.DataFrame()
    df = pd.DataFrame(data, columns=[
        'TaskName', 'State', 'StartTime', 'FinishTime', 'ElapsedTime',
        'AttemptStartTime', 'ShuffleFinishTime', 'MergeFinishTime', 'AttemptFinishTime',
        'ElapsedShuffle', 'ElapsedMerge', 'ElapsedReduce', 'AttemptElapsedTime'
    ])
    for col in ['StartTime', 'FinishTime', 'ElapsedTime', 'AttemptStartTime', 'ShuffleFinishTime', 'MergeFinishTime',
                'AttemptFinishTime', 'ElapsedShuffle', 'ElapsedMerge', 'ElapsedReduce', 'AttemptElapsedTime']:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0).astype(int)
    return df


def parse_counters(file_path):
    if not os.path.exists(file_path): return None
    try:
        df = pd.read_csv(file_path)
        if not df.empty: return df.iloc[0].to_dict()
    except:
        return None
    return None


def calculate_detailed_metrics(map_df, reduce_df, counters_dict=None):
    results = {}

    # 1. Map 阶段
    results['【Map阶段】任务总数'] = len(map_df) #map_tasks.csv 的行数
    if not map_df.empty:
        map_start = map_df['AttemptStartTime'].min() #最早开始
        map_end = map_df['AttemptFinishTime'].max() #最晚结束，列均在表格中
        results['【Map阶段】墙钟时间(秒)'] = round((map_end - map_start) / 1000.0, 2) #除于1000使单位变为s
        results['【Map阶段】平均任务耗时(秒)'] = round(map_df['AttemptElapsedTime'].mean() / 1000.0, 2)
        results['【Map阶段】累计CPU时间(秒)'] = round(map_df['AttemptElapsedTime'].sum() / 1000.0, 2)
    else:
        map_start, map_end = 0, 0

    # 2. Reduce 阶段
    results['【Reduce阶段】任务总数'] = len(reduce_df)
    shuffle_duration_sec = 0

    if len(reduce_df) == 1:
        row = reduce_df.iloc[0]
        shuffle_duration_sec = row['ElapsedShuffle'] / 1000.0
        results['【Reduce阶段】Copy/Shuffle耗时(秒)'] = round(shuffle_duration_sec, 2)
        results['【Reduce阶段】Sort/Merge耗时(秒)'] = round(row['ElapsedMerge'] / 1000.0, 2)
        results['【Reduce阶段】Reduce计算耗时(秒)'] = round(row['ElapsedReduce'] / 1000.0, 2)
        results['【Reduce阶段】总耗时(秒)'] = round(row['AttemptElapsedTime'] / 1000.0, 2)
    elif len(reduce_df) > 1:
        shuffle_duration_sec = reduce_df['ElapsedShuffle'].mean() / 1000.0
        results['【Reduce阶段】平均Copy/Shuffle(秒)'] = round(shuffle_duration_sec, 2)
        results['【Reduce阶段】平均Sort/Merge(秒)'] = round(reduce_df['ElapsedMerge'].mean() / 1000.0, 2)
        results['【Reduce阶段】平均Reduce计算(秒)'] = round(reduce_df['ElapsedReduce'].mean() / 1000.0, 2)
        results['【Reduce阶段】平均总耗时(秒)'] = round(reduce_df['AttemptElapsedTime'].mean() / 1000.0, 2)

    # 3. I/O 分析 (网络 + 磁盘)
    if counters_dict:
        # 网络 I/O
        shuffle_bytes = counters_dict.get('shuffle_bytes', 0)
        if shuffle_bytes > 0:
            shuffle_mb = shuffle_bytes / (1024 * 1024)
            results['【网络I/O】Shuffle传输总量(MB)'] = round(shuffle_mb, 2)
            if shuffle_duration_sec > 0.1:
                results['【网络I/O】Shuffle传输速率(MB/s)'] = round(shuffle_mb / shuffle_duration_sec, 2)

        # 磁盘 I/O (Spill)
        spill_records = counters_dict.get('spill_records', 0)
        results['【磁盘I/O】Spill(溢写)记录数'] = spill_records
        if spill_records == 0:
            results['【磁盘I/O】评价'] = "优秀 (内存充足，无溢写)"
        else:
            results['【磁盘I/O】评价'] = "存在磁盘溢写，内存压力较大"

    # 4. 整体 & 重叠度
    if not reduce_df.empty and not map_df.empty:
        job_start = min(map_start, reduce_df['AttemptStartTime'].min())
        job_end = max(map_end, reduce_df['AttemptFinishTime'].max())
        results['【整体作业】总耗时(秒)'] = round((job_end - job_start) / 1000.0, 2)

        # 重叠度
        total_overlap, total_shuffle = 0, 0
        for _, row in reduce_df.iterrows():
            s_start, s_end = row['AttemptStartTime'], row['ShuffleFinishTime']
            overlap_start = max(map_start, s_start)
            overlap_end = min(map_end, s_end)
            if overlap_start < overlap_end:
                total_overlap += (overlap_end - overlap_start)
            total_shuffle += (s_end - s_start)

        if total_shuffle > 0:
            results['【性能分析】Shuffle重叠度(%)'] = round((total_overlap / total_shuffle) * 100, 2)

    return results


def process_one_case(folder_path, case_name, f_log):
    map_file = os.path.join(folder_path, 'map_tasks.csv')
    reduce_file = os.path.join(folder_path, 'reduce_tasks.csv')
    counter_file = os.path.join(folder_path, 'counters_summary.csv')

    if not os.path.exists(map_file) or not os.path.exists(reduce_file): return False

    log("\n" + "-" * 50, f_log)
    log(f"CASE: {case_name}", f_log)
    log("-" * 50, f_log)

    map_df = parse_map_tasks(map_file)
    reduce_df = parse_reduce_tasks(reduce_file)
    counters_dict = parse_counters(counter_file)

    if map_df.empty or reduce_df.empty: return False

    results = calculate_detailed_metrics(map_df, reduce_df, counters_dict)
    for k, v in results.items(): log(f"{k:40s}: {v}", f_log)
    return True


def main():
    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.join(script_dir, 'data_csv')
    output_txt_path = os.path.join(script_dir, 'all_analysis_results.txt')

    if not os.path.exists(data_dir):
        print(f"❌ 找不到根目录: {data_dir}")
        return

    with open(output_txt_path, 'w', encoding='utf-8') as f_log:
        log("=" * 80, f_log)
        log(f"Hadoop 全维度分析报告 - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", f_log)
        log("=" * 80, f_log)

        for root, dirs, files in os.walk(data_dir):
            if 'map_tasks.csv' in files:
                case_name = os.path.relpath(root, data_dir)
                process_one_case(root, case_name, f_log)

    print(f"\n✓ 完成！结果已保存: {output_txt_path}")


if __name__ == '__main__':
    main()