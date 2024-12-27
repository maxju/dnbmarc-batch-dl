#!/bin/bash

MAX_IDLE_MINUTES=30
SCRIPT_PATH="convert.py"
LOG_FILE="converter.log"
HEARTBEAT_FILE="converter_heartbeat.txt"
LAST_RESTART=$(date +%s)
MIN_UPTIME_SECONDS=30  # Minimum time between restarts to prevent rapid restart loops
MAX_LOG_LINES=1000    # Maximum number of lines to keep in the log file
ROTATE_CHECK_INTERVAL=300  # Check log size every 5 minutes

rotate_logs() {
    local log_lines=$(wc -l < "$LOG_FILE")
    if [ "$log_lines" -gt "$MAX_LOG_LINES" ]; then
        echo "$(date): Log file exceeds $MAX_LOG_LINES lines. Rotating..." | tee -a $LOG_FILE
        tail -n $MAX_LOG_LINES "$LOG_FILE" > "$LOG_FILE.tmp" && mv "$LOG_FILE.tmp" "$LOG_FILE"
    fi
}

# Function to handle real-time log rotation
process_log_line() {
    while IFS= read -r line; do
        echo "$line" >> "$LOG_FILE"
        echo "$line"
        
        # Check log size after each write
        local log_lines=$(wc -l < "$LOG_FILE")
        if [ "$log_lines" -gt "$MAX_LOG_LINES" ]; then
            # Rotate logs without logging the rotation message (to avoid recursion)
            tail -n $MAX_LOG_LINES "$LOG_FILE" > "$LOG_FILE.tmp" && mv "$LOG_FILE.tmp" "$LOG_FILE"
        fi
    done
}

# Initial log rotation
if [ -f "$LOG_FILE" ]; then
    rotate_logs
fi

cleanup() {
    echo "$(date): Initiating cleanup..." | process_log_line
    
    # Kill all child processes in the process group
    if [ ! -z "$PID" ]; then
        echo "$(date): Terminating conversion process and its children..." | process_log_line
        pkill -TERM -P $PID 2>/dev/null
        sleep 2
        pkill -KILL -P $PID 2>/dev/null
        kill -KILL $PID 2>/dev/null
        wait $PID 2>/dev/null
    fi
    
    # Cleanup any stray Python processes
    echo "$(date): Cleaning up any remaining processes..." | process_log_line
    pkill -f "anaconda3/envs/dnb-converter/bin/python3.*multiprocessing" 2>/dev/null
    
    # Remove heartbeat file
    if [ -f "$HEARTBEAT_FILE" ]; then
        echo "$(date): Removing heartbeat file..." | process_log_line
        rm -f $HEARTBEAT_FILE
    fi
    
    echo "$(date): Cleanup complete. Exiting..." | process_log_line
    exit 0
}

# Trap various termination signals
trap cleanup SIGINT SIGTERM SIGQUIT SIGHUP

check_process_health() {
    # Check if heartbeat file exists and is recent
    if [ -f "$HEARTBEAT_FILE" ]; then
        HEARTBEAT_TIME=$(cat $HEARTBEAT_FILE | cut -d. -f1)  # Truncate to integer
        CURRENT_TIME=$(date +%s)
        DIFF_SECONDS=$((CURRENT_TIME - HEARTBEAT_TIME))
        
        if [ $DIFF_SECONDS -gt $((MAX_IDLE_MINUTES * 60)) ]; then
            echo "$(date): Process heartbeat too old ($DIFF_SECONDS seconds). Restarting..." | process_log_line
            return 1
        fi
    else
        echo "$(date): No heartbeat file found. Restarting..." | process_log_line
        return 1
    fi
    
    # Check if process is actually running
    if ! kill -0 $PID 2>/dev/null; then
        echo "$(date): Process not running. Restarting..." | process_log_line
        return 1
    fi
    
    return 0
}

while true; do
    # Start the conversion script with real-time log processing
    python3 $SCRIPT_PATH 2>&1 | process_log_line &
    PID=$!
    
    while true; do
        sleep 60  # Check every minute
        
        # Check if enough time has passed since last restart
        CURRENT_TIME=$(date +%s)
        UPTIME=$((CURRENT_TIME - LAST_RESTART))
        
        if ! check_process_health; then
            if [ $UPTIME -lt $MIN_UPTIME_SECONDS ]; then
                echo "$(date): Process failed too quickly. Waiting before restart..." | process_log_line
                sleep 30
            fi
            kill -9 $PID 2>/dev/null
            LAST_RESTART=$(date +%s)
            break
        fi
    done
    
    echo "$(date): Restarting process in 10 seconds..." | process_log_line
    sleep 10
done