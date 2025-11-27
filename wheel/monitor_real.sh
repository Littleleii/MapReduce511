#!/bin/bash

LOGFILE=$1

# Worker 节点列表
NODES=("worker1-zzh" "worker2-zrt" "worker3-haz")

echo "===== Real Performance Monitor Started at $(date) =====" >> "$LOGFILE"

start_time=$(date +%s)

#######################################
# 获取 CPU 使用率 - from /proc/stat
#######################################
get_cpu_usage() {
    ssh $1 "awk '/^cpu / {print \$2+\$3+\$4+\$5+\$6+\$7+\$8, \$5}' /proc/stat"
}

#######################################
# 获取内存占用率 - MemTotal - MemAvailable
#######################################
get_mem_usage() {
    ssh $1 "awk '
        /MemTotal/     {total=\$2}
        /MemAvailable/ {avail=\$2}
        END {printf(\"%.0f\", (total-avail)/total*100)}
    ' /proc/meminfo"
}

#######################################
# 主循环：监控直到 yarn job 结束
#######################################
while true; do
    
    RUNNING=$(yarn application -list 2>/dev/null | grep "RUNNING")
    if [[ -z "$RUNNING" ]]; then
        break
    fi

    for NODE in "${NODES[@]}"; do

        # 采样 CPU（两次差分法）
        read -r cpu1 idle1 <<< $(get_cpu_usage $NODE)
        sleep 1
        read -r cpu2 idle2 <<< $(get_cpu_usage $NODE)

        cpu_diff=$((cpu2 - cpu1))
        idle_diff=$((idle2 - idle1))

        if [[ $cpu_diff -gt 0 ]]; then
            cpu_usage=$(echo "scale=2; (1 - $idle_diff / $cpu_diff) * 100" | bc)
        else
            cpu_usage="0"
        fi

        # 采样内存
        mem_usage=$(get_mem_usage $NODE)

        echo "[$NODE] CPU: ${cpu_usage}% | MEM: ${mem_usage}%" >> "$LOGFILE"
    done

    echo "----" >> "$LOGFILE"
done

#######################################
# 作业结束，记录总耗时
#######################################
end_time=$(date +%s)
runtime=$((end_time - start_time))

echo "===== Job Finished =====" >> "$LOGFILE"
echo "===== Total Duration: ${runtime}s =====" >> "$LOGFILE"

