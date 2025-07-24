import sys
from hdfs import InsecureClient
from confluent_kafka import Producer
import threading

# 1️⃣ Configuration: map HDFS paths to Kafka topics
PATH_TOPIC_MAP = {
    '/user/multiple_tables/zomato.csv':    'zomato_topic',
    '/user/multiple_tables/mining.csv': 'mining_topic',
    '/user/multiple_tables/flights.csv':  'flights_topic',
}

# 2️⃣ Kafka producer config
producer_conf = {
    'bootstrap.servers': 'localhost:9092',
    'linger.ms': 0,            # no batching delay
    'queue.buffering.max.messages': 1000000,
    'enable.idempotence': True # safer delivery
}
producer = Producer(producer_conf)

# 3️⃣ HDFS client (adjust namenode URL as needed)
hdfs_client = InsecureClient('http://namenode-host:9870', user='hadoop')

def push_file_to_topic(hdfs_path: str, topic: str):
    """
    Reads the given HDFS file line-by-line and pushes
    each line as a separate Kafka record to `topic`.
    """
    print(f"▶️ Starting transfer of {hdfs_path} → {topic}")
    with hdfs_client.read(hdfs_path, encoding='utf-8') as reader:
        for line_num, line in enumerate(reader, start=1):
            # strip newline, or do CSV parsing here if needed
            record = line.rstrip('\n')
            
            # Synchronous send to partition 0 to preserve order
            producer.produce(
                topic=topic,
                key=None,         # or set key=filename if needed
                value=record,
                partition=0
            )
            producer.flush()     # wait for delivery before next record
            if line_num % 10000 == 0:
                print(f"  …sent {line_num} records")

    print(f"✅ Completed {hdfs_path} → {topic}")

def main():
    threads = []
    # Launch one thread per file → topic
    for path, topic in PATH_TOPIC_MAP.items():
        t = threading.Thread(target=push_file_to_topic, args=(path, topic))
        t.start()
        threads.append(t)

    # Wait for all to finish
    for t in threads:
        t.join()

    print("🎉 All files transferred.")

if __name__ == '__main__':
    main()