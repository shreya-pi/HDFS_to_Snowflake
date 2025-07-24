# kafka_migrator.py
import concurrent.futures
import queue

import datetime
from confluent_kafka import Producer
from pyhive import hive
import datetime
import os
import json

class KafkaMigrator:
    """
    A class to manage the migration of Hive tables to Kafka topics.
    """
    def __init__(self, kafka_brokers, hive_db, hive_host, hive_port, hive_user):
        self.kafka_brokers = kafka_brokers
        self.hive_db = hive_db
        self.hive_host = hive_host
        self.hive_port = hive_port
        self.hive_user = hive_user
        # self.state_file = state_file

        script_dir = os.path.dirname(os.path.abspath(__file__))
        # Join it with the desired filename to create a full, predictable path.
        # This ensures the file is always in your project's root directory.
        self.state_file = os.path.join(script_dir, 'kafka_migrator_state.json')
        self.state_manager = self._load_state()
        # self.state_manager = {}
        print("KafkaMigrator initialized.")


    def _load_state(self):
        """Loads the last processed dates from the state file."""
        if os.path.exists(self.state_file):
            with open(self.state_file, 'r') as f:
                try:
                    return json.load(f)
                except json.JSONDecodeError:
                    # If file is corrupted or empty, start fresh
                    return {}
        return {}

    def _save_state(self):
        """Saves the current state_manager dictionary to the state file atomically."""
        temp_file = self.state_file + '.tmp'
        with open(temp_file, 'w') as f:
            json.dump(self.state_manager, f, indent=4)
        # Atomic rename operation
        os.replace(temp_file, self.state_file)


    def _get_hive_connection(self):
        """Helper to create a Hive connection."""
        return hive.Connection(
            host=self.hive_host, port=self.hive_port,
            username=self.hive_user, database=self.hive_db
        )

    def get_hive_tables(self):
        """Connects to Hive to fetch and return a list of all table names."""
        try:
            with self._get_hive_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SHOW TABLES")
                return [row[0] for row in cursor.fetchall()]
        except Exception as e:
            raise RuntimeError(f"Failed to fetch tables from Hive: {e}")


    def get_partition_preview(self, table_name, limit=10):
        """
        Fetches a sample of data ONLY from the NEW partitions for a given table,
        based on the last processed date in the persistent state file.
        Returns a pandas DataFrame.
        """
        import pandas as pd
        log_prefix = f"[{table_name}]"

        # Step 1: Discover all available partitions from Hive.
        try:
            with self._get_hive_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f"SHOW PARTITIONS {self.hive_db}.{table_name}")
                all_partitions_str = [row[0] for row in cursor.fetchall()]
        except Exception as e:
            print(f"{log_prefix} Error fetching partitions for preview: {e}")
            return pd.DataFrame() # Return empty on error

        if not all_partitions_str:
            print(f"{log_prefix} No partitions found for preview.")
            return pd.DataFrame()

        all_dates = []
        for part_str in all_partitions_str:
            try:
                all_dates.append(part_str.split('load_date=')[1])
            except IndexError:
                continue

        # Step 2: Determine which partitions are NEW based on the persistent state.
        last_processed_date = self.state_manager.get(table_name)
        
        if not last_processed_date:
            partitions_to_preview = sorted(all_dates)
        else:
            partitions_to_preview = sorted([d for d in all_dates if d > last_processed_date])
            
        if not partitions_to_preview:
            # This is not an error, just means we're up-to-date.
            print(f"{log_prefix} No new partitions to preview.")
            return pd.DataFrame({'status': [f'Table is up-to-date. Last processed date: {last_processed_date}']})

        # Step 3: Construct the query to fetch data ONLY from the new partitions.
        fully_qualified_table = f"{self.hive_db}.{table_name}"
        where_clause = " OR ".join([f"load_date='{p}'" for p in partitions_to_preview])
        query = f"SELECT * FROM {fully_qualified_table} WHERE {where_clause} LIMIT {limit}"
        
        print(f"{log_prefix} Previewing data from partitions: {', '.join(partitions_to_preview)}")
        
        try:
            with self._get_hive_connection() as conn:
                df = pd.read_sql(query, conn)
                if df.empty:
                    return pd.DataFrame({'status': [f'Found new partitions, but they contain no data.']})
                return df
        except Exception as e:
            print(f"{log_prefix} Error running preview query: {e}")
            return pd.DataFrame()
        

    def discover_new_partitions(self, table_name):
        log_prefix = f"[{table_name}]"
        yield f"{log_prefix} Discovering new partitions..."
        last_processed_date = self.state_manager.get(table_name)
        yield f"{log_prefix} Last persisted processed date: {last_processed_date}"
        fully_qualified_table = f"{self.hive_db}.{table_name}"
        try:
            with self._get_hive_connection() as conn:
                cursor = conn.cursor()
                cursor.execute(f"SHOW PARTITIONS {fully_qualified_table}")
                all_partitions = [row[0] for row in cursor.fetchall()]
        except Exception as e:
            yield f"{log_prefix} ❌ Could not get partitions: {e}"
            yield []
            return
        if not all_partitions:
            yield f"{log_prefix} No partitions found for this table."
            yield []
            return
        all_dates = []
        for part_str in all_partitions:
            try:
                date_str = part_str.split('load_date=')[1]
                all_dates.append(date_str)
            except IndexError: continue
        if not last_processed_date:
            partitions_to_process = sorted(all_dates)
            yield f"{log_prefix} No previous state found. Will process all {len(partitions_to_process)} partitions."
        else:
            partitions_to_process = sorted([d for d in all_dates if d > last_processed_date])
            yield f"{log_prefix} Found {len(partitions_to_process)} new partitions to process."
        yield partitions_to_process


    def stream_delta_to_kafka(self, table_name, partitions):
        """
        Streams ONLY the data from the specified new partitions to Kafka.
        NO LONGER SENDS A HEADER.
        """
        log_prefix = f"[{table_name}]"
        if not partitions:
            yield f"{log_prefix} No new partitions to stream. Skipping."
            return

        yield f"{log_prefix} --- Starting DELTA stream for {len(partitions)} partition(s) ---"

        load_date_str = datetime.date.today().isoformat()
        yield f"{log_prefix} stamping all new records with load_date: {load_date_str}"
        
        try:
            producer = Producer({'bootstrap.servers': self.kafka_brokers})
            yield f"{log_prefix} 🔌 Kafka producer connected."
            
            with self._get_hive_connection() as conn:
                yield f"{log_prefix} 🛰️ Hive connection successful."
                cursor = conn.cursor()
                
                fully_qualified_table = f"{self.hive_db}.{table_name}"
                topic_name = f"{table_name}_delta_hive_topic"
                
                # Construct a WHERE clause to select only the new partitions
                where_clause = " OR ".join([f"load_date='{p}'" for p in partitions])
                query = f"SELECT * FROM {fully_qualified_table} WHERE {where_clause}"
                
                yield f"{log_prefix} 🚀 Streaming data to Kafka topic: {topic_name}"
                yield f"{log_prefix} Query: SELECT * FROM ... WHERE load_date IN (...)"

                cursor.execute(query)
                
                row_count = 0
                for row in cursor:
                    # # Exclude the partition column from the output message if it's the last one
                    # row_to_send = row[:-1]
                    # row_str = ','.join([str(field) if field is not None else '' for field in row_to_send])
                    fields_to_send = list(row[:-1])
                    # 2. Append the new system date as the last field
                    fields_to_send.append(load_date_str)
                    # 3. Join into the final string
                    row_str = ','.join([str(field) if field is not None else '' for field in fields_to_send])
                    
                    # No header is produced, just the data
                    producer.produce(topic_name, value=row_str.encode('utf-8'))
                    row_count += 1

                    if row_count % 5000 == 0:
                        yield f"{log_prefix} ✈️  ...sent {row_count} new records..."
                        producer.poll(0)
                
                producer.flush()
                yield f"{log_prefix} ✅ Done! Flushed {row_count} new records to Kafka."

                # Update the state to the latest processed partition
                latest_partition = partitions[-1]
                self.state_manager[table_name] = latest_partition

                self._save_state()
                yield f"{log_prefix} 🖋️  State updated. Last processed date is now: {latest_partition}"

        except Exception as e:
            yield f"{log_prefix} ❌ An error occurred: {e}"
            
        yield f"{log_prefix} --- Delta stream finished ---"


    def run_streaming_in_parallel(self, tables_with_partitions):
        """
        Orchestrates the concurrent streaming of multiple tables to Kafka using a thread pool.
        Yields interleaved log messages from all running threads.
        """
        log_queue = queue.Queue()
        tasks_to_run = {table: parts for table, parts in tables_with_partitions.items() if parts}

        if not tasks_to_run:
            yield "No tables with new data to process."
            return

        # The worker function that each thread will execute
        def worker(table, partitions):
            """Calls the streaming generator and puts its logs onto the queue."""
            try:
                for log_line in self.stream_delta_to_kafka(table, partitions):
                    log_queue.put(log_line)
            except Exception as e:
                # Put any unexpected errors on the queue as well
                log_queue.put(f"[{table}] ❌ A critical error occurred in the worker thread: {e}")
            finally:
                # Use a sentinel value to signal that this worker is done
                log_queue.put(f"__DONE__{table}")

        yield f"🚀 Starting concurrent streaming for {len(tasks_to_run)} table(s)..."
        
        with concurrent.futures.ThreadPoolExecutor(max_workers=len(tasks_to_run)) as executor:
            # Submit all tasks to the thread pool
            for table, partitions in tasks_to_run.items():
                executor.submit(worker, table, partitions)

            tasks_done = 0
            # Poll the queue for logs until all tasks have signaled they are done
            while tasks_done < len(tasks_to_run):
                try:
                    # Get a log message from the queue, with a timeout to prevent blocking forever
                    log_line = log_queue.get(timeout=10)
                    
                    if log_line.startswith("__DONE__"):
                        tasks_done += 1
                    else:
                        yield log_line
                except queue.Empty:
                    # If the queue is empty for too long, it might mean threads have died.
                    yield "[Orchestrator] Waiting for logs... (if this persists, a thread may have failed silently)"

        yield "🏁 All streaming threads have completed."



