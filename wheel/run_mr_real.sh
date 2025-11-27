#!/bin/bash

##############################################
# Run MapReduce + Real Performance Monitoring
##############################################

INPUT=$1
OUTPUT=$2
SLOWSTART=$3

if [ -z "$INPUT" ] || [ -z "$OUTPUT" ] || [ -z "$SLOWSTART" ]; then
    echo "Usage: ./run_mr_real.sh <input_path> <output_path> <slowstart>"
    echo "Example: ./run_mr_real.sh /wiki1G/AA/* /_100mb 0.2"
    exit 1
fi

###############################
# 1) 创建日志目录（包含 slowstart）
###############################
OUTNAME=$(basename "$OUTPUT")   # 如 "/_100mb" → "_100mb"
LOG_DIR="$HOME/code/MapReduceLog/${OUTNAME}_slowstart_${SLOWSTART}"

mkdir -p "$LOG_DIR"
echo "[INFO] Log directory: $LOG_DIR"

###############################
# 2) 启动监控 monitor_real.sh
###############################
MONITOR_LOG="$LOG_DIR/monitor.log"

bash "$HOME/code/wheel/monitor_real.sh" "$MONITOR_LOG" "$SLOWSTART" &
MONITOR_PID=$!
echo "[INFO] Performance monitor started, PID=$MONITOR_PID"

sleep 1

###############################
# 3) 运行 MapReduce 并记录输出
###############################
JOB_LOG="$LOG_DIR/job_output.log"

{
    echo "===== Running MapReduce Job ====="
    echo "Input : $INPUT"
    echo "Output: $OUTPUT"
    echo "Slowstart: $SLOWSTART"
    echo "Date  : $(date)"
    echo "================================="
} > "$JOB_LOG"

# 删除旧输出（避免冲突）
hdfs dfs -rm -r -f "$OUTPUT" >/dev/null 2>&1

# 运行 WordCount（加入 slowstart）
hadoop jar $HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-examples*.jar \
    wordcount \
    -D mapreduce.job.reduce.slowstart.completedmaps=$SLOWSTART \
    "$INPUT" "$OUTPUT" 2>&1 | tee -a "$JOB_LOG"

echo "[INFO] MapReduce job finished."

###############################
# 4) 停止监控
###############################
kill $MONITOR_PID >/dev/null 2>&1
wait $MONITOR_PID 2>/dev/null

echo "[INFO] Monitor stopped."
echo "[INFO] All logs saved to $LOG_DIR"

exit 0

