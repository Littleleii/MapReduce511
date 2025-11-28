#!/bin/bash

##############################################
# Run MapReduce + Real Performance Monitoring
##############################################

INPUT=$1
OUTPUT=$2
SLOWSTART=$3

if [ -z "$INPUT" ] || [ -z "$OUTPUT" ] || [ -z "$SLOWSTART" ]; then
    echo "Usage: ./run_mr_real.sh <input_path> <output_path> <slowstart>"
    exit 1
fi

###############################
# 使用外部传入的 RUN_LOG_DIR
###############################
if [ -z "$RUN_LOG_DIR" ]; then
    echo "[ERROR] RUN_LOG_DIR is not set! This script must be called from run_batch.sh"
    exit 1
fi

mkdir -p "$RUN_LOG_DIR"
echo "[INFO] Log directory for this run: $RUN_LOG_DIR"

MONITOR_LOG="$RUN_LOG_DIR/monitor.log"
JOB_LOG="$RUN_LOG_DIR/job_output.log"

###############################
# 启动监控
###############################
bash "$HOME/code/wheel/monitor_real.sh" "$MONITOR_LOG" &
MONITOR_PID=$!
echo "[INFO] Performance monitor started, PID=$MONITOR_PID"
sleep 1

###############################
# 运行 MapReduce
###############################
{
    echo "===== Running MapReduce Job ====="
    echo "Input : $INPUT"
    echo "Output: $OUTPUT"
    echo "Slowstart: $SLOWSTART"
    echo "Date  : $(date)"
    echo "================================="
} > "$JOB_LOG"

# 删除旧 HDFS 输出（避免冲突）
hdfs dfs -rm -r -f "$OUTPUT" >/dev/null 2>&1

hadoop jar /usr/local/hadoop/share/hadoop/mapreduce/hadoop-mapreduce-examples-3.3.4.jar \
    wordcount \
    -D mapreduce.job.reduce.slowstart.completedmaps=$SLOWSTART \
    -D mapreduce.input.fileinputformat.input.dir.recursive=true \
    "$INPUT" "$OUTPUT" 2>&1 | tee -a "$JOB_LOG"

echo "[INFO] MapReduce job finished." | tee -a "$JOB_LOG"

###############################
# 停止监控
###############################
kill $MONITOR_PID >/dev/null 2>&1
wait $MONITOR_PID 2>/dev/null

echo "[INFO] Monitor stopped." | tee -a "$JOB_LOG"
echo "[INFO] Logs saved to: $RUN_LOG_DIR"

exit 0

