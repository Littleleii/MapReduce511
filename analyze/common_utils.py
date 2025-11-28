import os
import re
import datetime
import pandas as pd
import numpy as np


def parse_monitor_log(log_path):
    """解析单个 monitor.log 文件"""
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


def parse_job_stages(job_log_path):
    """解析 job_output.log 获取阶段时间信息（修复 Reduce 时间）"""
    if not os.path.exists(job_log_path):
        return None

    with open(job_log_path, 'r', encoding='utf-8', errors='ignore') as f:
        content = f.read()

    prog_pattern = re.compile(
        r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}),\d+\s+INFO\s+mapreduce\.Job:\s+map\s+(\d+)%\s+reduce\s+(\d+)%',
        re.MULTILINE
    )

    matches = prog_pattern.findall(content)
    if not matches:
        return None

    records = []
    for ts_str, map_p, red_p in matches:
        t = datetime.datetime.strptime(ts_str, '%Y-%m-%d %H:%M:%S')
        records.append({
            'time': t,
            'map_pct': int(map_p),
            'red_pct': int(red_p)
        })

    records.sort(key=lambda x: x['time'])

    if not records:
        return None

    t0 = records[0]['time']

    # 1. Map 完成时间
    t_map_done = None
    for r in records:
        if r['map_pct'] == 100:
            t_map_done = r['time']
            break

    if t_map_done is None:
        return None

    # 2. Shuffle 开始时间
    t_shuf_start = None
    for r in records:
        if r['red_pct'] > 0:
            t_shuf_start = r['time']
            break

    # 3. Shuffle 结束 / Reduce 开始时间
    # 使用启发式：Map完成后，Reduce进度达到90%以上
    t_shuf_end = None
    t_reduce_start = None

    for r in records:
        if r['map_pct'] == 100 and r['red_pct'] >= 90:
            t_shuf_end = r['time']
            t_reduce_start = r['time']
            break

    # 如果没找到，使用倒数第二条记录作为Shuffle结束
    if t_shuf_end is None:
        if len(records) >= 2:
            t_shuf_end = records[-2]['time']
            t_reduce_start = records[-2]['time']
        else:
            t_shuf_end = records[-1]['time']
            t_reduce_start = records[-1]['time']

    # 4. 任务完成时间 
    t_job_done = records[-1]['time']

    # 5. 计算各阶段耗时
    total_time = (t_job_done - t0).total_seconds()
    map_time = (t_map_done - t0).total_seconds()

    if t_shuf_start and t_shuf_end:
        shuffle_duration = (t_shuf_end - t_shuf_start).total_seconds()
    else:
        shuffle_duration = 0.0
        t_shuf_start = t_map_done
        t_shuf_end = t_map_done

    reduce_time = (t_job_done - t_reduce_start).total_seconds()

    #  6. 计算重叠比例
    if shuffle_duration > 0:
        overlap_start = max(t0, t_shuf_start)
        overlap_end = min(t_map_done, t_shuf_end)

        if overlap_end > overlap_start:
            overlap_time = (overlap_end - overlap_start).total_seconds()
        else:
            overlap_time = 0.0

        overlap_ratio = (overlap_time / shuffle_duration) * 100.0
    else:
        overlap_ratio = 0.0

    return {
        'Map耗时(s)': round(map_time, 2),
        'Shuffle耗时(s)': round(shuffle_duration, 2),
        'Reduce耗时(s)': round(reduce_time, 2),
        '总耗时(s)': round(total_time, 2),
        'Shuffle重叠比(%)': round(overlap_ratio, 2)
    }

def scan_multiple_runs(base_dir='./MapReduceLog'):
    """
    扫描多轮实验数据（新目录结构）
    """
    if not os.path.exists(base_dir):
        print(f"错误：找不到目录 {base_dir}")
        return {}, {}

    monitor_data = {}
    stage_data = {}

    print(f"扫描目录: {base_dir}")

    # 遍历第一层：数据集_slowstart_值
    for dataset_folder in os.listdir(base_dir):
        dataset_path = os.path.join(base_dir, dataset_folder)

        if not os.path.isdir(dataset_path):
            continue

        # 解析文件夹名：100mb_slowstart_0.2
        match = re.search(r'_?(\d+(?:mb|MB|gb|GB|M|G)?)_slowstart_([\d\.]+)', dataset_folder, re.IGNORECASE)

        if not match:
            print(f"  跳过非标准文件夹: {dataset_folder}")
            continue

        dataset = match.group(1).upper()  # 统一转大写
        slowstart = float(match.group(2))

        print(f"\n处理: {dataset} - SlowStart {slowstart}")

        if dataset not in monitor_data:
            monitor_data[dataset] = {}
            stage_data[dataset] = {}
        if slowstart not in monitor_data[dataset]:
            monitor_data[dataset][slowstart] = []
            stage_data[dataset][slowstart] = []

        # 遍历第二层：时间戳文件夹
        run_folders = [f for f in os.listdir(dataset_path)
                       if os.path.isdir(os.path.join(dataset_path, f))]

        run_folders.sort()

        print(f"  发现 {len(run_folders)} 次实验运行")

        for run_idx, run_folder in enumerate(run_folders, 1):
            run_path = os.path.join(dataset_path, run_folder)

            monitor_log = os.path.join(run_path, 'monitor.log')
            if os.path.exists(monitor_log):
                df = parse_monitor_log(monitor_log)
                if not df.empty:
                    min_time = df['Time_Step'].min()
                    df['Time_Step'] = df['Time_Step'] - min_time
                    monitor_data[dataset][slowstart].append(df)
                    print(f"    [Monitor] 第 {run_idx} 次 - ✓ 数据点: {len(df)}")
                else:
                    print(f"    [Monitor] 第 {run_idx} 次 - ✗ 空数据")
            else:
                print(f"    [Monitor] 第 {run_idx} 次 - ✗ 文件缺失")

            job_log = os.path.join(run_path, 'job_output.log')
            if os.path.exists(job_log):
                stage_info = parse_job_stages(job_log)
                if stage_info:
                    stage_data[dataset][slowstart].append(stage_info)
                    print(f"    [Stage]   第 {run_idx} 次 - ✓ 总耗时: {stage_info['总耗时(s)']:.1f}s")
                else:
                    print(f"    [Stage]   第 {run_idx} 次 - ✗ 解析失败")
            else:
                print(f"    [Stage]   第 {run_idx} 次 - ✗ 文件缺失")

    print("\n" + "=" * 60)
    print("扫描完成统计:")
    for dataset in sorted(monitor_data.keys()):
        print(f"\n数据集: {dataset}")
        for slowstart in sorted(monitor_data[dataset].keys()):
            monitor_count = len(monitor_data[dataset][slowstart])
            stage_count = len(stage_data[dataset][slowstart])
            print(f"  SlowStart {slowstart}: Monitor={monitor_count}次, Stage={stage_count}次")

    return monitor_data, stage_data


def average_monitor_data(monitor_data):
    """
    将多轮 monitor 数据平均（CPU）
    """
    SAMPLING_INTERVAL = 1  # ← 添加这一行

    averaged_data = {}

    for dataset, ss_dict in monitor_data.items():
        averaged_data[dataset] = {}

        for slowstart, df_list in ss_dict.items():
            if not df_list:
                continue

            print(f"  平均化 {dataset} SS:{slowstart} - {len(df_list)} 轮数据")

            time_cpu_values = {}

            for df in df_list:
                cpu_by_time = df.groupby('Time_Step')['CPU'].mean()
                for time_step, cpu_val in cpu_by_time.items():
                    if time_step not in time_cpu_values:
                        time_cpu_values[time_step] = []
                    time_cpu_values[time_step].append(cpu_val)

            averaged_records = []
            for time_step in sorted(time_cpu_values.keys()):
                avg_cpu = np.mean(time_cpu_values[time_step])
                averaged_records.append({
                    'Time_Step': time_step * SAMPLING_INTERVAL,  # ← 修改这一行
                    'CPU': avg_cpu
                })

            if averaged_records:
                averaged_data[dataset][slowstart] = pd.DataFrame(averaged_records)

    return averaged_data

def average_monitor_data_mem(monitor_data):
    """
    将多轮 monitor 数据平均（MEM）
    """
    averaged_data = {}

    for dataset, ss_dict in monitor_data.items():
        averaged_data[dataset] = {}

        for slowstart, df_list in ss_dict.items():
            if not df_list:
                continue

            print(f"  平均化内存数据 {dataset} SS:{slowstart} - {len(df_list)} 轮")

            time_mem_values = {}

            for df in df_list:
                mem_by_time = df.groupby('Time_Step')['MEM'].mean()
                for time_step, mem_val in mem_by_time.items():
                    if time_step not in time_mem_values:
                        time_mem_values[time_step] = []
                    time_mem_values[time_step].append(mem_val)

            averaged_records = []
            for time_step in sorted(time_mem_values.keys()):
                avg_mem = np.mean(time_mem_values[time_step])
                averaged_records.append({
                    'Time_Step': time_step,
                    'MEM': avg_mem
                })

            if averaged_records:
                averaged_data[dataset][slowstart] = pd.DataFrame(averaged_records)

    return averaged_data


def average_stage_data(stage_data):
    """
    将多轮 stage 数据平均
    """
    averaged_stages = {}

    for dataset, ss_dict in stage_data.items():
        averaged_stages[dataset] = {}

        for slowstart, stage_list in ss_dict.items():
            if not stage_list:
                continue

            print(f"  平均化阶段数据 {dataset} SS:{slowstart} - {len(stage_list)} 轮")

            avg_stage = {}
            for key in stage_list[0].keys():
                values = [stage[key] for stage in stage_list if key in stage]
                avg_stage[key] = np.mean(values) if values else 0.0

            averaged_stages[dataset][slowstart] = avg_stage

    return averaged_stages


def sort_dataset_key(name):
    """数据集排序键函数"""
    num = re.search(r'\d+', name)
    v = int(num.group()) if num else 0
    if 'G' in name.upper():
        v *= 1000
    return v


if __name__ == '__main__':
    print(" common_utils.py 加载成功")
    print("可用函数:")
    print("  - scan_multiple_runs")
    print("  - average_monitor_data")
    print("  - average_monitor_data_mem")
    print("  - average_stage_data")
    print("  - parse_monitor_log")
    print("  - parse_job_stages")
    print("  - sort_dataset_key")
