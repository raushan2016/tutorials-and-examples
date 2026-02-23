#!/bin/bash
set -e

# Configuration
TARGET_RUNS=30
SELECTOR="cloud.google.com/gke-accelerator=nvidia-b200"
RESET_THRESHOLD_DAYS="-1" # Force reset
YAML_FILE="gpu-reset-job.yaml"

# Identification
echo "Finding B200 nodes using selector: $SELECTOR"
NODES=$(kubectl get nodes -l "$SELECTOR" -o jsonpath='{.items[*].metadata.name}')

if [ -z "$NODES" ]; then
    echo "No B200 nodes found."
    exit 1
fi

RUN_ID=$(date +%s)
echo "Run ID: $RUN_ID"

NODE_ARRAY=($NODES)
NUM_NODES=${#NODE_ARRAY[@]}
echo "Found $NUM_NODES nodes: ${NODE_ARRAY[*]}"

# check yaml
if [ ! -f "$YAML_FILE" ]; then
    echo "Error: $YAML_FILE not found."
    exit 1
fi

# Calculate batches
BATCHES=$(( (TARGET_RUNS + NUM_NODES - 1) / NUM_NODES ))
echo "Targeting $TARGET_RUNS runs. Will run $BATCHES batches on $NUM_NODES nodes."

# Tracking
JOB_TRACK_DIR=$(mktemp -d)
echo "Tracking jobs in $JOB_TRACK_DIR"
RESULTS_FILE="$JOB_TRACK_DIR/results.csv"
echo "job_name,node,start_time,completion_time,duration_seconds" > "$RESULTS_FILE"

cleanup() {
    echo "Cleaning up..."
    # Optional: Delete jobs could happen here
}
trap cleanup EXIT

# Main Loop
for ((BATCH=1; BATCH<=BATCHES; BATCH++)); do
    echo "========================================"
    echo "Starting Batch $BATCH / $BATCHES"
    echo "========================================"
    
    BATCH_JOB_NAMES=()
    
    # Submit Jobs for this batch
    for NODE in "${NODE_ARRAY[@]}"; do
        # Generate job
        JOB_NAME=$(cat "${YAML_FILE}" | \
          sed "s/##NODE_NAME##/${NODE}/g" | \
          sed "s/##RESET_THRESHOLD_DAYS##/${RESET_THRESHOLD_DAYS}/g" | \
          sed "s/generateName: gpu-reset-manual-job-/generateName: gpu-reset-${RUN_ID}-batch-${BATCH}-/g" | \
          kubectl create -f - -o json | jq -r '.metadata.name')
          
        echo "$JOB_NAME" > "$JOB_TRACK_DIR/$NODE.jobname"
        BATCH_JOB_NAMES+=("$JOB_NAME")
        echo "Submitted $JOB_NAME for $NODE (Batch $BATCH)"
    done
    
    # Wait for completion of this batch
    echo "Waiting for batch $BATCH to complete..."
    
    while true; do
        ALL_DONE=true
        PENDING=0
        RUNNING=0
        
        for NODE in "${NODE_ARRAY[@]}"; do
             if [ ! -f "$JOB_TRACK_DIR/$NODE.batch$BATCH.done" ]; then
                JOB_NAME=$(cat "$JOB_TRACK_DIR/$NODE.jobname")
                
                # Check status safe
                JOB_JSON=$(kubectl get job "$JOB_NAME" -o json 2>/dev/null || true)
                
                if [ -z "$JOB_JSON" ]; then
                    # Job might be deleted or failed to create?
                    echo "Warning: Job $JOB_NAME not found."
                    touch "$JOB_TRACK_DIR/$NODE.batch$BATCH.done"
                    continue
                fi
                
                SUCCEEDED=$(echo "$JOB_JSON" | jq -r '.status.succeeded // 0')
                FAILED=$(echo "$JOB_JSON" | jq -r '.status.failed // 0')
                
                if [ "$SUCCEEDED" -eq 1 ] || [ "$FAILED" -ge 1 ]; then
                     # Job finished
                     START_TIME=$(echo "$JOB_JSON" | jq -r '.status.startTime')
                     COMPLETION_TIME=$(echo "$JOB_JSON" | jq -r '.status.completionTime')
                     
                     if [ "$START_TIME" != "null" ] && [ "$COMPLETION_TIME" != "null" ]; then
                         S_SEC=$(date -d "$START_TIME" +%s)
                         E_SEC=$(date -d "$COMPLETION_TIME" +%s)
                         DURATION=$((E_SEC - S_SEC))
                         
                         echo "$JOB_NAME,$NODE,$START_TIME,$COMPLETION_TIME,$DURATION" >> "$RESULTS_FILE"
                         echo "Job $JOB_NAME (Node $NODE) finished in ${DURATION}s"
                     else
                         echo "Job $JOB_NAME (Node $NODE) finished but missing times. S/F: $SUCCEEDED/$FAILED"
                     fi
                     
                     touch "$JOB_TRACK_DIR/$NODE.batch$BATCH.done"
                     
                     # Optional: Cleanup completed job now to save resources?
                     # kubectl delete job "$JOB_NAME" --wait=false >/dev/null 2>&1 || true
                else
                    ALL_DONE=false
                     ACTIVE=$(echo "$JOB_JSON" | jq -r '.status.active // 0')
                    if [ "$ACTIVE" -ge 1 ]; then
                        RUNNING=$((RUNNING + 1))
                    else
                        PENDING=$((PENDING + 1))
                    fi
                fi
             fi
        done
        
        if $ALL_DONE; then
            echo "Batch $BATCH completed."
            break
        fi
        
        echo "Batch $BATCH Running: $RUNNING, Pending: $PENDING"
        sleep 5
    done
    
    # Optional wait between batches?
    # sleep 5
done

echo "Calculating stats..."

DURATIONS=$(awk -F, 'NR>1 {print $5}' "$RESULTS_FILE" | sort -n)
COUNT=$(echo "$DURATIONS" | wc -l)

if [ "$COUNT" -eq 0 ]; then
    echo "No valid durations captured."
    exit 0
fi

get_percentile() {
    local p=$1
    local idx=$(awk -v c="$COUNT" -v p="$p" 'BEGIN { x = c * p / 100; i = int(x); if (x > i) i+=1; if (i==0) i=1; print i }')
    echo "$DURATIONS" | sed -n "${idx}p"
}

P50=$(get_percentile 50)
P90=$(get_percentile 90)
P99=$(get_percentile 99)

echo "------------------------------------------------"
echo "Benchmark Results ($COUNT runs)"
echo "P50 Duration: ${P50}s"
echo "P90 Duration: ${P90}s"
echo "P99 Duration: ${P99}s"
echo "------------------------------------------------"
echo "Detailed results in $RESULTS_FILE"
