#!/usr/bin/env bash
#
# hdfs_kafka_sync.sh — Monitor HDFS CSV and push only new lines to Kafka
 
####### CONFIGURATION ########
HDFS_PATH="file_path_in_hdfs" # e.g., "/user/hadoop/myfile.csv"
OFFSET_FILE="$HOME/.name_offset"
KAFKA_HOME="$HOME/kafka"                     # Adjust if Kafka is in a subfolder
BOOTSTRAP="localhost:9092" # Adjust to your Kafka bootstrap server
TOPIC="topic_name" # Adjust to your topic name
POLL_INTERVAL=60                             # in seconds
LOG_FILE="$HOME/hdfs_kafka_sync.log"
##############################
 
# Init offset file if it doesn't exist
[ -f "$OFFSET_FILE" ] || echo "0" > "$OFFSET_FILE"
 
# Add Kafka to PATH if not already available
#export PATH="$KAFKA_HOME/bin:$PATH"
 
echo "=== Starting HDFS→Kafka sync ==="
while true; do
  TIMESTAMP=$(date '+%Y-%m-%d %H:%M:%S')
 
  # 1. Count current lines in the file
# CURRENT_TOTAL=$(hdfs dfs -cat "$HDFS_PATH" 2>/dev/null | wc -l)
# LAST_TOTAL=$(< "$OFFSET_FILE")
# DELTA=$(( CURRENT_TOTAL - LAST_TOTAL ))
 
 
  CURRENT_TOTAL=$(hdfs dfs -cat "$HDFS_PATH" | wc -l)
  LAST_TOTAL=$(< "$OFFSET_FILE")
  DELTA=$(( CURRENT_TOTAL - LAST_TOTAL ))
 
  echo "$TIMESTAMP – DEBUG: CURRENT_TOTAL=$CURRENT_TOTAL, LAST_TOTAL=$LAST_TOTAL, DELTA=$DELTA" >> "$LOG_FILE"

  # 2. If new lines exist, tail and push to Kafka
  if (( DELTA > 0 )); then
    echo "$TIMESTAMP — Detected $DELTA new lines" >> "$LOG_FILE"
    hdfs dfs -cat "$HDFS_PATH" 2>/dev/null \
      | tail -n "$DELTA" \
      | kafka-console-producer.sh \
          --broker-list "$BOOTSTRAP" \
          --topic "$TOPIC"
 
    echo "$CURRENT_TOTAL" > "$OFFSET_FILE"
    echo "$TIMESTAMP — Pushed $DELTA new lines, offset updated to $CURRENT_TOTAL" \
>> "$LOG_FILE"
  else
    echo "$TIMESTAMP — No new lines detected" >> "$LOG_FILE"
  fi
 
  sleep "$POLL_INTERVAL"
done