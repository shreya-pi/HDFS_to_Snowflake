import threading
import snowflake.connector
from confluent_kafka import Consumer, KafkaException, KafkaError
import time
import os
import re
import csv
import io
from concurrent.futures import ThreadPoolExecutor
from config import SNOWFLAKE_CONFIG

# --- Configuration (remains the same) ---
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
TOPICS = ['sales_topic', 'productivity_topic', 'cust_topic']
BATCH_SIZE = 100
COMMIT_INTERVAL_SECONDS = 5
MAX_DDL_WORKERS = 5
PHASE_1_TIMEOUT_SECONDS = 60

# --- Helper Functions (with the correction) ---

def get_table_name_from_topic(topic_name):
    if topic_name.endswith('_topic'):
        return topic_name[:-len('_topic')]
    return topic_name

def sanitize_name(name):
    s = name.strip().upper()
    s = re.sub(r'[^A-Z0-9_]', '_', s)
    if not s or s[0].isdigit():
        s = f"COL_{s}"
    return s



def create_table_in_snowflake(topic_name, header_columns):
    """
    Connects to Snowflake and creates a table.
    FIXED: Handles column names in 'table.column' format correctly.
    """
    thread_id = threading.get_ident()
    base_name = get_table_name_from_topic(topic_name)
    table_name = sanitize_name(base_name)
    print(f"THREAD {thread_id} [{topic_name}]: DDL task started for table '{table_name}'.")

    used_colnames = set()
    formatted_columns = []
    for col in header_columns:
        # --- THIS IS THE FIX ---
        # If the column name is like 'tablename.colname', split it and take the last part.
        clean_col_name = col.split('.')[-1]

        # Now, sanitize the truly clean column name
        final_col = sanitize_name(clean_col_name)

        # Handle cases where different columns sanitize to the same name
        counter = 1
        original_col = final_col
        while final_col in used_colnames:
            final_col = f"{original_col}_{counter}"
            counter += 1
        used_colnames.add(final_col)
        formatted_columns.append(f'"{final_col}" STRING')

    ddl = f"CREATE OR REPLACE TABLE {table_name} ({', '.join(formatted_columns)})"

    try:
        with snowflake.connector.connect(**SNOWFLAKE_CONFIG) as conn:
            with conn.cursor() as cursor:
                cursor.execute(ddl)
                print(f"THREAD {thread_id} [{topic_name}]: ✅ Table '{table_name}' created successfully.")
                return True
    except Exception as e:
        print(f"THREAD {thread_id} [{topic_name}]: ❌ DDL for table '{table_name}' failed: {e}")
        return False


def phase1_create_tables(topics_to_process):
    print("\n--- PHASE 1: Schema Discovery & Table Creation ---")

    consumer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': f'schema-discovery-group-{os.getpid()}',
        'auto.offset.reset': 'earliest'
    }
    consumer = Consumer(consumer_config)
    consumer.subscribe(topics_to_process)

    processed_topics = set()
    successful_topics = []
    lock = threading.Lock()

    def mark_success(topic):
        with lock: successful_topics.append(topic)

    with ThreadPoolExecutor(max_workers=MAX_DDL_WORKERS) as executor:
        start_time = time.time()
        while len(processed_topics) < len(topics_to_process):
            if time.time() - start_time > PHASE_1_TIMEOUT_SECONDS:
                print("⏰ PHASE 1: Timeout reached.")
                break

            msg = consumer.poll(1.0)
            if msg is None: continue
            if msg.error():
                if msg.error().code() != KafkaError._PARTITION_EOF: print(f"[{msg.topic()}] Kafka error in Phase 1: {msg.error()}")
                continue

            topic = msg.topic()
            if topic in processed_topics: continue

            print(f"[{topic}] Found header message. Submitting DDL task.")
            processed_topics.add(topic)

            try:
                header_str = msg.value().decode('utf-8')
                header_cols = next(csv.reader(io.StringIO(header_str)))
                future = executor.submit(create_table_in_snowflake, topic, header_cols)
                future.add_done_callback(lambda f, t=topic: mark_success(t) if f.result() else None)
            except Exception as e:
                print(f"[{topic}] ❌ Could not parse header: {e}")

    consumer.close()
    print("--- PHASE 1: Complete ---")
    return successful_topics

# --- Phase 2: Ingest Data (No changes needed here) ---

def phase2_consume_and_insert(topic_name):
    base_name = get_table_name_from_topic(topic_name)
    table_name = sanitize_name(base_name)
    print(f"[{topic_name}] Starting ingestion thread for table '{table_name}'...")

    consumer_config = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'snowflake-data-ingestion-group-v1',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    }
    consumer = Consumer(consumer_config)
    consumer.subscribe([topic_name])

    conn = snowflake.connector.connect(**SNOWFLAKE_CONFIG)
    cursor = conn.cursor()
    print(f"[{topic_name}] Connected to Snowflake for ingestion.")

    batch = []
    last_commit_time = time.time()
    header_skipped = False

    try:
        while True:
            msg = consumer.poll(1.0)

            if not header_skipped and msg:
                print(f"[{topic_name}] Skipping header row.")
                header_skipped = True
                consumer.commit(asynchronous=False)
                continue

            if msg is None:
                if batch and (time.time() - last_commit_time > COMMIT_INTERVAL_SECONDS):
                    print(f"[{topic_name}] Interval reached. Inserting batch of {len(batch)}...")
                    insert_batch_into_snowflake(cursor, table_name, batch)
                    conn.commit()
                    consumer.commit(asynchronous=False)
                    batch = []
                    last_commit_time = time.time()
                continue

            if msg.error():
                print(f"[{topic_name}] Kafka error in Phase 2: {msg.error()}")
                continue

            row_values = [v.strip() for v in msg.value().decode('utf-8').strip().split(',')]
            batch.append(row_values)

            if len(batch) >= BATCH_SIZE:
                print(f"[{topic_name}] Batch size reached. Inserting batch of {len(batch)}...")
                insert_batch_into_snowflake(cursor, table_name, batch)
                conn.commit()
                consumer.commit(asynchronous=False)
                batch = []
                last_commit_time = time.time()

    except KeyboardInterrupt:
        print(f"[{topic_name}] Stopping consumer...")
    finally:
        if batch:
            print(f"[{topic_name}] Committing final batch of {len(batch)} records...")
            insert_batch_into_snowflake(cursor, table_name, batch)
            conn.commit()
            consumer.commit(asynchronous=False)
        print(f"[{topic_name}] Closing connections.")
        consumer.close()
        cursor.close()
        conn.close()


def insert_batch_into_snowflake(cursor, table_name, batch):
    if not batch: return
    try:
        placeholders = ','.join(['%s'] * len(batch[0]))
        sql = f"INSERT INTO {table_name} VALUES ({placeholders})"
        cursor.executemany(sql, batch)
    except Exception as e:
        print(f"[{table_name}] ❌ Batch insert failed: {e}")

# --- Main Orchestrator (No changes needed here) ---

def main():
    successful_topics = phase1_create_tables(TOPICS)

    if not successful_topics:
        print("\nNo tables were created successfully. Exiting.")
        return

    print(f"\n--- PHASE 2: Data Ingestion for topics: {', '.join(successful_topics)} ---")
    threads = []
    for topic in successful_topics:
        thread = threading.Thread(target=phase2_consume_and_insert, args=(topic,))
        thread.daemon = True
        thread.start()
        threads.append(thread)

    try:
        while True: time.sleep(1)
    except KeyboardInterrupt:
        print("\nMain thread caught KeyboardInterrupt. Shutting down...")

if __name__ == "__main__":
    main()
