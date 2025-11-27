#!/bin/bash

###################################
# Batch run for multiple slowstart
###################################

INPUT=$1
OUTPUT=$2

if [ -z "$INPUT" ] || [ -z "$OUTPUT" ]; then
    echo "Usage: ./run_batch.sh <input_path> <output_path>"
    echo "Example: ./run_batch.sh /wiki1G/* /_1G"
    exit 1
fi

SLOWSTART_VALUES=(0.2 0.5 0.8 1.0)

for SS in "${SLOWSTART_VALUES[@]}"; do
    echo "============================================"
    echo " Running slowstart = $SS "
    echo "============================================"

    ./run_mr_real.sh "$INPUT" "$OUTPUT" "$SS"

    echo
    echo "[INFO] slowstart $SS completed."
    echo
done

echo "============================================"
echo " All experiments finished!"
echo " Logs saved under ~/code/MapReduceLog/"
echo "============================================"

exit 0

