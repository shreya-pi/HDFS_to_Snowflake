#!/usr/bin/env python3
import subprocess, sys, datetime
from confluent_kafka import Producer
 
# ——— CONFIGURATION ———
HDFS_BASE      = "file_path"   # your CSVs live here
STATE_FILE     = f"{HDFS_BASE}/_state/last_date.txt"  # state tracking file
KAFKA_BROKERS  = "localhost:9092"     # update to your actual broker(s)
KAFKA_TOPIC    = "topic_name"
 
# ——— HELPERS ———
def hdfs_list(path):
    """List files/dirs under an HDFS path."""
    out = subprocess.check_output(["hdfs", "dfs", "-ls", path]).decode()
    return [ line.split()[-1] for line in out.splitlines() if "Found" not in line ]
 
def read_hdfs_text(path):
    """Slurp an HDFS text file, return lines."""
    raw = subprocess.check_output(["hdfs", "dfs", "-cat", path]).decode()
    return raw.splitlines()
 
def read_last_date():
    """Return last date processed, or None."""
    try:
        txt = subprocess.check_output(["hdfs", "dfs", "-cat", STATE_FILE]).decode().strip()
        return datetime.date.fromisoformat(txt)
    except subprocess.CalledProcessError:
        return None
 
def write_last_date(d):
    """Overwrite STATE_FILE on HDFS with date d."""
    p = subprocess.Popen(
        ["hdfs", "dfs", "-put", "-f", "-", STATE_FILE],
        stdin = subprocess.PIPE
    )
    p.communicate(input=d.isoformat().encode())
 
# ——— MAIN ———
def main():
    last = read_last_date()
    today = datetime.date.today()
    start = (last + datetime.timedelta(days=1)) if last else today

    # 1) list all date=YYYY-MM-DD dirs
    parts = hdfs_list(HDFS_BASE)
    all_dates = sorted(
        datetime.date.fromisoformat(p.split("date=")[1])
        for p in parts if "date=" in p
    )
    to_proc = [d for d in all_dates if start <= d <= today]
    if not to_proc:
        print("⏭️  No new partitions.")
        return
 
    # 2) Configure Kafka producer
    producer = Producer({"bootstrap.servers": KAFKA_BROKERS})
 
    # 3) For each date, push its files
    for d in to_proc:
        folder = f"{HDFS_BASE}/date={d.isoformat()}"
        files  = hdfs_list(folder)
        for f in files:
            for line in read_hdfs_text(f):
                producer.produce(KAFKA_TOPIC, value=line)
        producer.flush()
        write_last_date(d)
        print(f"✅  Processed partition {d.isoformat()}")
 
if __name__ == "__main__":
    main()