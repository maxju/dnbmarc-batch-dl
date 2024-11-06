#!/bin/bash

MAX_IDLE_MINUTES=15
SCRIPT_PATH="convert.py"
LOG_FILE="converter.log"

cleanup() {
    echo "Stopping conversion process..."
    if [ ! -z "$PID" ]; then
        # Beende alle Child-Prozesse
        pkill -P $PID
        # Warte kurz
        sleep 1
        # Falls noch welche übrig sind, härter beenden
        pkill -9 -P $PID 2>/dev/null
        # Hauptprozess beenden
        kill $PID 2>/dev/null
        wait $PID 2>/dev/null
    fi
    # Für den Fall, dass noch Python-Prozesse aus unserem Environment übrig sind
    pkill -f "anaconda3/envs/dnb-converter/bin/python3.*multiprocessing" 2>/dev/null
    exit 0
}

# SIGINT (CTRL+C) und SIGTERM abfangen
trap cleanup SIGINT SIGTERM

while true; do
    # Start the conversion script and show output in terminal
    python3 $SCRIPT_PATH 2>&1 &
    PID=$!
    
    while kill -0 $PID 2>/dev/null; do
        # Check the latest file in uploaded folder
        LATEST_FILE=$(find temp_files/uploaded -type f -printf '%T@ %p\n' 2>/dev/null | sort -n | tail -1)
        if [ ! -z "$LATEST_FILE" ]; then
            FILE_TIME=$(echo $LATEST_FILE | cut -d' ' -f1 | cut -d'.' -f1)
            CURRENT_TIME=$(date +%s)
            DIFF_MINUTES=$(( (CURRENT_TIME - FILE_TIME) / 60 ))
            
            if [ $DIFF_MINUTES -gt $MAX_IDLE_MINUTES ]; then
                echo "$(date): No new files for $DIFF_MINUTES minutes. Restarting process..." | tee -a $LOG_FILE
                kill $PID
                break
            fi
        fi
        sleep 60  # Check every minute
    done
    
    # Wenn cleanup ausgeführt wurde, nicht neu starten
    if [ $? -eq 0 ]; then
        echo "$(date): Process ended or killed. Restarting in 10 seconds..." | tee -a $LOG_FILE
        sleep 10
    else
        break
    fi
done