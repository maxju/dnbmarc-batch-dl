#!/bin/bash

MAX_IDLE_MINUTES=15
SCRIPT_PATH="convert.py"
LOG_FILE="converter.log"
HEARTBEAT_FILE="converter_heartbeat.txt"
LAST_RESTART=$(date +%s)
MIN_UPTIME_SECONDS=30  # Minimum time between restarts to prevent rapid restart loops

cleanup() {
    echo "Stopping conversion process..."
    if [ ! -z "$PID" ]; then
        pkill -P $PID
        sleep 2
        pkill -9 -P $PID 2>/dev/null
        kill -9 $PID 2>/dev/null
        wait $PID 2>/dev/null
    fi
    pkill -f "anaconda3/envs/dnb-converter/bin/python3.*multiprocessing" 2>/dev/null
    rm -f $HEARTBEAT_FILE
    exit 0
}

trap cleanup SIGINT SIGTERM

check_process_health() {
    # Check if heartbeat file exists and is recent
    if [ -f "$HEARTBEAT_FILE" ]; then
        HEARTBEAT_TIME=$(cat $HEARTBEAT_FILE)
        CURRENT_TIME=$(date +%s)
        DIFF_SECONDS=$((CURRENT_TIME - HEARTBEAT_TIME))
        
        if [ $DIFF_SECONDS -gt $((MAX_IDLE_MINUTES * 60)) ]; then
            echo "$(date): Process heartbeat too old ($DIFF_SECONDS seconds). Restarting..." | tee -a $LOG_FILE
            return 1
        fi
    else
        echo "$(date): No heartbeat file found. Restarting..." | tee -a $LOG_FILE
        return 1
    fi
    
    # Check if process is actually running
    if ! kill -0 $PID 2>/dev/null; then
        echo "$(date): Process not running. Restarting..." | tee -a $LOG_FILE
        return 1
    fi
    
    return 0
}

while true; do
    # Start the conversion script
    python3 $SCRIPT_PATH 2>&1 | tee -a $LOG_FILE &
    PID=$!
    
    while true; do
        sleep 60  # Check every minute
        
        # Check if enough time has passed since last restart
        CURRENT_TIME=$(date +%s)
        UPTIME=$((CURRENT_TIME - LAST_RESTART))
        
        if ! check_process_health; then
            if [ $UPTIME -lt $MIN_UPTIME_SECONDS ]; then
                echo "$(date): Process failed too quickly. Waiting before restart..." | tee -a $LOG_FILE
                sleep 30
            fi
            kill -9 $PID 2>/dev/null
            LAST_RESTART=$(date +%s)
            break
        fi
    done
    
    echo "$(date): Restarting process in 10 seconds..." | tee -a $LOG_FILE
    sleep 10
done