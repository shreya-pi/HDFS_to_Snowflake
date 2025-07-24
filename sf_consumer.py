# snowflake_consumer.py

import threading
import snowflake.connector
from confluent_kafka import Consumer, KafkaError, TopicPartition
import time
import re
import csv
import io
from concurrent.futures import ThreadPoolExecutor

class SnowflakeConsumer:
    """
    A class to manage consuming data from Kafka topics and ingesting it
    into corresponding Snowflake tables.
    """
    def __init__(self, kafka_brokers, snowflake_config, batch_size=100, commit_interval=5):
        self.kafka_brokers = kafka_brokers
        self.snowflake_config = snowflake_config
        self.batch_size = batch_size
        self.commit_interval = commit_interval
        print("SnowflakeConsumer initialized.")

    def _sanitize_name(self, name):
        """Private helper to create valid Snowflake object names."""
        s = name.strip().upper()
        s = re.sub(r'[^A-Z0-9_]', '_', s)
        if not s or s[0].isdigit():
            s = f"COL_{s}"
        return s

    def _get_table_name_from_topic(self, topic_name):
        """Derives a table name from a topic name."""
        if topic_name.endswith('delta_hive_topic'):
            base_name = topic_name[:-len('delta_hive_topic')]
        elif topic_name.endswith('delta_hdfs_topic'):
            base_name = topic_name[:-len('delta_hdfs_topic')]
        else:
            base_name = topic_name
        return self._sanitize_name(base_name)


    def _create_table_in_snowflake(self, topic_name, header_columns):
        """
        Connects to Snowflake and creates a table based on header columns.
        Yields log messages during the process.
        """
        table_name = self._get_table_name_from_topic(topic_name)
        log_prefix = f"[{table_name}]"
        
        yield f"{log_prefix} DDL task started."

        used_colnames, formatted_columns = set(), []
        for col in header_columns:
            clean_col_name = col.split('.')[-1]
            final_col = self._sanitize_name(clean_col_name)
            counter = 1
            original_col = final_col
            while final_col in used_colnames:
                final_col = f"{original_col}_{counter}"
                counter += 1
            used_colnames.add(final_col)
            formatted_columns.append(f'"{final_col}" STRING')
        
        ddl = f"CREATE OR REPLACE TABLE {table_name} ({', '.join(formatted_columns)})"
        
        try:
            with snowflake.connector.connect(**self.snowflake_config) as conn:
                with conn.cursor() as cursor:
                    cursor.execute(ddl)
                    yield f"{log_prefix} ✅ Table created successfully."
                    return True
        except Exception as e:
            yield f"{log_prefix} ❌ DDL for table failed: {e}"
            return False

    def discover_and_create_tables(self, topics, timeout=30):
        """
        Phase 1: Consumes the first message from each topic to infer schema
        and create tables in Snowflake.
        """
        yield "--- PHASE 1: Schema Discovery & Table Creation ---"
        
        consumer_config = {
            'bootstrap.servers': self.kafka_brokers,
            'group.id': f'schema-discovery-group-{time.time()}',
            'auto.offset.reset': 'earliest'
        }
        consumer = Consumer(consumer_config)
        consumer.subscribe(topics)
        
        processed_topics = set()
        successful_topics = []
        futures = {}

        with ThreadPoolExecutor(max_workers=len(topics)) as executor:
            start_time = time.time()
            while len(processed_topics) < len(topics) and time.time() - start_time < timeout:
                msg = consumer.poll(1.0)
                if msg is None: continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        yield f"[{msg.topic()}] Kafka error in Phase 1: {msg.error()}"
                    continue

                topic = msg.topic()
                if topic in processed_topics: continue
                
                processed_topics.add(topic)
                yield f"[{topic}] Found header message. Submitting DDL task."
                
                try:
                    header_str = msg.value().decode('utf-8')
                    header_cols = next(csv.reader(io.StringIO(header_str)))
                    # Submit the DDL creation and store the future
                    future = executor.submit(list, self._create_table_in_snowflake(topic, header_cols))
                    futures[topic] = future
                except Exception as e:
                    yield f"[{topic}] ❌ Could not parse header: {e}"
        
        # After polling, check the results of the DDL operations
        for topic, future in futures.items():
            try:
                # The result() call will block until the thread is done
                logs = future.result()
                for log in logs: yield log # Stream back the logs from the thread
                # The last log will contain success or failure info
                if '✅' in logs[-1]:
                    successful_topics.append(topic)
            except Exception as e:
                yield f"[{topic}] ❌ DDL future failed: {e}"

        consumer.close()
        yield f"--- PHASE 1: Complete. Successful tables: {', '.join(successful_topics) or 'None'} ---"
        yield successful_topics
        # return successful_topics

    # --- NEW ORCHESTRATION METHOD FOR BATCH MODE ---
    def run_batch_ingestion(self, topics_to_process, stop_event):
        """
        Orchestrates a batch ingestion run that stops when all messages are consumed.
        """
        if not topics_to_process:
            yield "No topics to process."
            return

        yield "--- PHASE 2: Starting Batch Ingestion ---"
        
        # Step 1: Get the current high-water marks for all topic partitions
        admin_consumer = Consumer({
            'bootstrap.servers': self.kafka_brokers,
            'group.id': f'admin-group-{time.time()}' # Use a temporary group
        })
        
        target_offsets = {}
        yield "🔎 Determining end-offsets for all topic partitions..."
        for topic in topics_to_process:
            metadata = admin_consumer.list_topics(topic, timeout=10)
            if metadata.topics[topic].error:
                yield f"❌ Could not get metadata for topic {topic}: {metadata.topics[topic].error}"
                continue
            for part_id, part_meta in metadata.topics[topic].partitions.items():
                low, high = admin_consumer.get_watermark_offsets(TopicPartition(topic, part_id), timeout=5)
                target_offsets[f"{topic}-{part_id}"] = high
                yield f"   - Topic '{topic}', Partition {part_id}: Target Offset = {high}"
        admin_consumer.close()

        if not target_offsets:
            yield "🤷 No partitions found for the given topics. Nothing to do."
            return

        # Step 2: Start consumer threads
        threads = []
        # Use a thread-safe dictionary to track the current position of each consumer
        consumed_offsets = {}
        lock = threading.Lock()

        def consumer_target(topic):
            for _ in self.consume_and_ingest(topic, stop_event, consumed_offsets, lock):
                # This inner generator will now just run. We don't need its logs here.
                pass
        
        for topic in topics_to_process:
            thread = threading.Thread(target=consumer_target, args=(topic,))
            thread.daemon = True
            thread.start()
            threads.append(thread)

        # Step 3: Monitor progress and decide when to stop
        yield "\n🚀 Ingestion threads started. Monitoring progress..."
        all_caught_up = False
        while not all_caught_up and not stop_event.is_set():
            time.sleep(5) # Check every 5 seconds
            
            with lock:
                # Assume we are done unless we find a partition that is not caught up
                all_caught_up = True
                progress_log = ["\n📊 Current Ingestion Progress:"]
                for key, target_offset in target_offsets.items():
                    current_offset = consumed_offsets.get(key, -1) + 1 # +1 because offset is 0-indexed
                    progress_log.append(f"   - {key}: Consumed {current_offset} / {target_offset}")
                    if current_offset < target_offset:
                        all_caught_up = False
                yield "\n".join(progress_log)

        if all_caught_up:
            yield "\n🎉 All topics have reached their high-water marks!"
        
        yield "🛑 Sending stop signal to all consumer threads..."
        stop_event.set()

        for t in threads:
            t.join() # Wait for all threads to finish cleanly

        yield "✅ All ingestion threads have stopped. Batch process complete."


    # --- MODIFIED CONSUME_AND_INGEST METHOD ---
    def consume_and_ingest(self, topic_name, stop_event, consumed_offsets=None, lock=None):
        # ... (This method has a slightly modified signature and internal logic) ...
        table_name = self._get_table_name_from_topic(topic_name)
        log_prefix = f"[{table_name}]"
        
        consumer_config = {
            'bootstrap.servers': self.kafka_brokers,
            'group.id': f'snowflake-ingestion-{table_name}',
            'auto.offset.reset': 'earliest',
            'enable.auto.commit': False
        }
        consumer = Consumer(consumer_config)
        consumer.subscribe([topic_name])
        
        try:
            conn = snowflake.connector.connect(**self.snowflake_config)
            cursor = conn.cursor()
        except Exception as e:
            # For simplicity, we just print here. In UI mode, this would be a yielded error.
            print(f"{log_prefix} ❌ Could not connect to Snowflake: {e}. Stopping thread.")
            consumer.close()
            return
        
        batch = []
        last_commit_time = time.time()
        
        while not stop_event.is_set():
            msg = consumer.poll(1.0)
            
            if msg is None:
                if batch and (time.time() - last_commit_time > self.commit_interval):
                    self._insert_batch(cursor, table_name, batch, conn, consumer, consumed_offsets, lock)
                    batch = []
                    last_commit_time = time.time()
                continue
            
            if msg.error():
                print(f"{log_prefix} Kafka error: {msg.error()}")
                continue
            
            is_header = False
            if msg.headers():
                for key, value in msg.headers():
                    if key == 'is_header' and value == b'true':
                        is_header = True
                        break
            
            if is_header:
                consumer.commit(asynchronous=False)
                continue

            try:
                row_values = next(csv.reader(io.StringIO(msg.value().decode('utf-8'))))
                # Add the message itself to the batch to access offset info later
                batch.append((msg, row_values))
            except Exception:
                continue

            if len(batch) >= self.batch_size:
                self._insert_batch(cursor, table_name, batch, conn, consumer, consumed_offsets, lock)
                batch = []
                last_commit_time = time.time()

        if batch:
            self._insert_batch(cursor, table_name, batch, conn, consumer, consumed_offsets, lock)

        consumer.close()
        cursor.close()
        conn.close()

    def _insert_batch(self, cursor, table_name, batch, conn, consumer, consumed_offsets, lock):
        if not batch: return
        try:
            # Extract just the row data for insertion
            data_to_insert = [item[1] for item in batch]
            num_cols = len(data_to_insert[0])
            placeholders = ','.join(['%s'] * num_cols)
            sql = f'INSERT INTO "{table_name}" VALUES ({placeholders})'
            
            
            valid_batch = [row for row in data_to_insert if len(row) == num_cols]
            
            if valid_batch:
                cursor.executemany(sql, valid_batch)
                conn.commit()
                consumer.commit(asynchronous=False)
                
                # --- KEY CHANGE: Update the global offset tracker ---
                if consumed_offsets is not None and lock is not None:
                    with lock:
                        for msg, _ in batch:
                            key = f"{msg.topic()}-{msg.partition()}"
                            consumed_offsets[key] = msg.offset()
        except Exception as e:
            print(f"[{table_name}] ❌ Batch insert failed: {e}")