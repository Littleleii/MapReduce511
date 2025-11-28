#!/bin/bash

###################################
# Batch run for multiple slowstart
###################################

INPUT=$1
OUTPUT=$2

if [ -z "$INPUT" ] || [ -z "$OUTPUT" ]; then
    echo "Usage: ./run_batch.sh <input_path> <output_path>"
    exit 1
fi

# slowstart 参数
SLOWSTART_VALUES=(0.2 0.5 0.8 1.0)

# 每个 slowstart 跑几次
RUNS_PER_SS=3

BASE_LOG_DIR="$HOME/code/MapReduceLog"
OUTNAME=$(basename "$OUTPUT")   # _100mb

for SS in "${SLOWSTART_VALUES[@]}"; do

    echo "============================================"
    echo " Running slowstart = $SS "
    echo "============================================"

    # 主目录，例如：_100mb_slowstart_0.2
    SS_DIR="${BASE_LOG_DIR}/${OUTNAME}_slowstart_${SS}"
    mkdir -p "$SS_DIR"

    for ((i=1; i<=RUNS_PER_SS; i++)); do
        echo "---- Run $i for slowstart=$SS ----"

        # 时间戳子目录
        TS=$(date +"%Y%m%d_%H%M%S")
        RUN_DIR="${SS_DIR}/${TS}"
        mkdir -p "$RUN_DIR"

        echo "[INFO] Logs for this run: $RUN_DIR"

        # 传入 RUN_LOG_DIR 环境变量
        RUN_LOG_DIR="$RUN_DIR" \
            ./run_mr_real.sh "$INPUT" "$OUTPUT" "$SS"

        echo "[INFO] slowstart $SS, run $i completed."
        echo
        sleep 2
    done

done

echo "============================================"
echo " All experiments finished!"
echo " Logs saved under: $BASE_LOG_DIR"
echo "============================================"

exit 0

