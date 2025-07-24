# hdfs_to_kafka.py

import subprocess
import datetime
from confluent_kafka import Producer

class HdfsToKafkaProducer:
    """
    A class to manage streaming new data from HDFS delta-load folders
    directly to corresponding Kafka topics.
    """
    def __init__(self, kafka_brokers, hdfs_base_path):
        self.kafka_brokers = kafka_brokers
        self.hdfs_base_path = hdfs_base_path
        print("HdfsToKafkaProducer initialized.")

    def _run_command(self, cmd_list):
        """Private helper to run shell commands."""
        try:
            output = subprocess.check_output(cmd_list, stderr=subprocess.STDOUT)
            return output.decode()
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Command failed: {' '.join(cmd_list)}\nError: {e.output.decode()}")

    def discover_delta_folders(self):
        """Scans the HDFS base path for folders ending in '_delta_load'."""
        try:
            all_items = self._run_command(["hdfs", "dfs", "-ls", "-C", self.hdfs_base_path]).strip().splitlines()
            delta_folders = sorted([item for item in all_items if item.endswith("_delta_load")])
            return delta_folders
        except Exception as e:
            raise RuntimeError(f"Failed to scan HDFS path {self.hdfs_base_path}:\n{e}")

    def stream_folder_to_kafka(self, delta_folder_path):
        """
        Generator to find new partitions in a delta folder and stream their
        contents to a corresponding Kafka topic.
        """
        folder_name = delta_folder_path.split('/')[-1]
        table_name = folder_name.replace("_delta_load", "")
        topic_name = f"{table_name}_delta_hdfs_topic"
        state_file = f"{delta_folder_path}/_state/last_date.txt" # Use a separate state file
        log_prefix = f"[{table_name}]"
        
        yield f"{log_prefix} --- Starting HDFS to Kafka stream for topic: {topic_name} ---"

        # 1. Read last processed date for this specific process
        last_processed_date = None
        try:
            self._run_command(["hdfs", "dfs", "-mkdir", "-p", f"{delta_folder_path}/_state"])
            txt = self._run_command(["hdfs", "dfs", "-cat", state_file]).strip()
            last_processed_date = datetime.date.fromisoformat(txt)
            yield f"{log_prefix} 📖 Last processed date for Kafka streaming: {last_processed_date}"
        except RuntimeError as e:
            if "No such file or directory" in str(e):
                yield f"{log_prefix} 📖 State file '{state_file}' not found. Will process all available partitions."
            else:
                yield f"{log_prefix} ❌ Error reading state file: {e}. Skipping."
                return

        # 2. Discover new date partitions in the HDFS folder
        try:
            all_paths = self._run_command(["hdfs", "dfs", "-ls", "-C", delta_folder_path]).strip().splitlines()
            all_dates = []
            for path in all_paths:
                if "date=" in path:
                    try:
                        date_str = path.split("date=")[1]
                        all_dates.append(datetime.date.fromisoformat(date_str))
                    except (ValueError, IndexError):
                        yield f"{log_prefix} ⚠️ Skipping malformed directory: {path}"
            
            if not last_processed_date:
                partitions_to_process = sorted(all_dates)
            else:
                partitions_to_process = sorted([d for d in all_dates if d > last_processed_date])

            if not partitions_to_process:
                yield f"{log_prefix} ✅ All up to date. No new partitions found."
                return
            yield f"{log_prefix} 🗓️ Found {len(partitions_to_process)} new partition(s) to process."

        except Exception as e:
            yield f"{log_prefix} ❌ Error listing HDFS partitions: {e}. Skipping."
            return

        # 3. Configure Kafka producer and stream data
        try:
            producer = Producer({"bootstrap.servers": self.kafka_brokers})
            yield f"{log_prefix} 🔌 Kafka producer connected."

            for d in partitions_to_process:
                partition_date_str = d.isoformat()
                partition_folder = f"{delta_folder_path}/date={partition_date_str}"
                yield f"{log_prefix}   - Processing partition: {partition_date_str}"

                files_in_partition = self._run_command(["hdfs", "dfs", "-ls", "-C", partition_folder]).strip().splitlines()
                
                total_lines = 0
                for hdfs_file in files_in_partition:
                    lines = self._run_command(["hdfs", "dfs", "-cat", hdfs_file]).strip().splitlines()
                    for line in lines:
                        # Produce message to Kafka. We assume each line is a message.
                        producer.produce(topic_name, value=line.encode('utf-8'))
                    total_lines += len(lines)
                
                yield f"{log_prefix}     ✈️  Sent {total_lines} lines from {len(files_in_partition)} file(s)."
                producer.flush() # Flush after each partition for robustness

                # 4. Update state file *after* a partition is successfully processed
                write_cmd = ["hdfs", "dfs", "-put", "-f", "-", state_file]
                p = subprocess.Popen(write_cmd, stdin=subprocess.PIPE, text=True)
                p.communicate(input=partition_date_str)
                yield f"{log_prefix}     🖋️  State file updated to {partition_date_str}."

            yield f"{log_prefix} --- ✅ Stream finished successfully! ---"

        except Exception as e:
            yield f"{log_prefix} ❌ An error occurred during streaming: {e}"








# #!/usr/bin/env python3
# import subprocess, sys, datetime
# from confluent_kafka import Producer
 
# # ——— CONFIGURATION ———
# HDFS_BASE      = "file_path"   # your CSVs live here
# STATE_FILE     = f"{HDFS_BASE}/_state/last_date.txt"  # state tracking file
# KAFKA_BROKERS  = "localhost:9092"     # update to your actual broker(s)
# KAFKA_TOPIC    = "topic_name"
 
# class HDFSDeltaProducer:
#      # ——— HELPERS ———
#      def hdfs_list(self,path):
#          """List files/dirs under an HDFS path."""
#          out = subprocess.check_output(["hdfs", "dfs", "-ls", path]).decode()
#          return [ line.split()[-1] for line in out.splitlines() if "Found" not in line ]
      
#      def read_hdfs_text(self,path):
#          """Slurp an HDFS text file, return lines."""
#          raw = subprocess.check_output(["hdfs", "dfs", "-cat", path]).decode()
#          return raw.splitlines()
      
#      def read_last_date(self):
#          """Return last date processed, or None."""
#          try:
#              txt = subprocess.check_output(["hdfs", "dfs", "-cat", STATE_FILE]).decode().strip()
#              return datetime.date.fromisoformat(txt)
#          except subprocess.CalledProcessError:
#              return None
      
#      def write_last_date(self,d):
#          """Overwrite STATE_FILE on HDFS with date d."""
#          p = subprocess.Popen(
#              ["hdfs", "dfs", "-put", "-f", "-", STATE_FILE],
#              stdin = subprocess.PIPE
#          )
#          p.communicate(input=d.isoformat().encode())
      
#      # ——— MAIN ———
#      def main(self):
#          last = self.read_last_date()
#          today = datetime.date.today()
#          start = (last + datetime.timedelta(days=1)) if last else today
     
#          # 1) list all date=YYYY-MM-DD dirs
#          parts = self.hdfs_list(HDFS_BASE)
#          all_dates = sorted(
#              datetime.date.fromisoformat(p.split("date=")[1])
#              for p in parts if "date=" in p
#          )
#          to_proc = [d for d in all_dates if start <= d <= today]
#          if not to_proc:
#              print("⏭️  No new partitions.")
#              return
      
#          # 2) Configure Kafka producer
#          producer = Producer({"bootstrap.servers": KAFKA_BROKERS})
      
#          # 3) For each date, push its files
#          for d in to_proc:
#              folder = f"{HDFS_BASE}/date={d.isoformat()}"
#              files  = self.hdfs_list(folder)
#              for f in files:
#                  for line in self.read_hdfs_text(f):
#                      producer.produce(KAFKA_TOPIC, value=line)
#              producer.flush()
#              self.write_last_date(d)
#              print(f"✅  Processed partition {d.isoformat()}")
 
# if __name__ == "__main__":
#     HDFSDeltaProducer().main()
    
    