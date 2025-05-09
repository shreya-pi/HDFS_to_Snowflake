# HDFS to Kafka to Snowflake Data Pipeline

This repository provides scripts and configurations to build a data pipeline that streams data from HDFS to Kafka and publishes it to Snowflake using the Snowflake Kafka Connector.

## Project Structure

- **[`delta_hdfs_kafka_connector.py`](delta_hdfs_kafka_connector.py)**: Python script to stream data from HDFS to Kafka using date-based partitions.
- **[`scripts.sh`](scripts.sh)**: Shell script with commands to set up and manage the pipeline, including Kafka topic creation, data streaming, and Snowflake integration.
- **[`SF_connect.properties`](SF_connect.properties)**: Configuration file for the Snowflake Kafka Connector.

---

## Prerequisites

1. **HDFS**: Ensure HDFS is set up and accessible.
2. **Kafka**: Install and configure Kafka.
3. **Snowflake**: Set up a Snowflake account and create the target table.
4. **Python**: Install Python 3.x and required libraries:
    ```bash
    pip install confluent-kafka
    ```

---

## Setup Instructions

### 1. Start Kafka Services
Start Zookeeper and Kafka servers:
```bash
kafka/bin/zookeeper-server-start.sh kafka/config/zookeeper.properties
kafka/bin/kafka-server-start.sh kafka/config/server.properties
```

### 2. Create a Kafka Topic
Create a Kafka topic to publish data:
```bash
kafka-topics.sh --create --topic <topic_name> --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1
```

### 3. Stream Data from HDFS to Kafka
Stream data from an HDFS file to the Kafka topic:
```bash
hdfs dfs -cat /path_to_file/file_name.csv | kafka-console-producer.sh --broker-list localhost:9092 --topic <topic_name>
```

### 4. Verify Data in Kafka
Check the messages pushed to the Kafka topic:
```bash
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic <topic_name> --from-beginning
```

### 5. Publish Data to Snowflake
Use the Snowflake Kafka Connector to publish data from Kafka to Snowflake:
```bash
bin/connect-standalone.sh config/connect-standalone.properties SF_connect.properties
```

Ensure the target table in Snowflake is created with the following format:
```sql
CREATE OR REPLACE TABLE DB_NAME.SCHEMA_NAME.TABLE_NAME (
     RECORD_METADATA VARIANT,
     RECORD_CONTENT VARIANT
);
```

---

## Scripts Overview

### `delta_hdfs_kafka_connector.py`
This script streams data from HDFS to Kafka using date-based partitions. It tracks the last processed date using a state file in HDFS.

Run the script:
```bash
python3 delta_hdfs_kafka_connector.py
```

### `scripts.sh`
This script includes commands to:
- Start Kafka services
- Create Kafka topics
- Stream data from HDFS to Kafka
- Verify Kafka messages
- Publish data to Snowflake

### `SF_connect.properties`
This file contains configuration details for the Snowflake Kafka Connector, including:
- Snowflake account details
- Kafka topic-to-table mapping
- Buffer settings

---

## Delta Load Options

1. **Using Date Partitions**: Run the `delta_hdfs_kafka_connector.py` script to process new date-based partitions.
2. **Using Line Counts**: Use a custom script `hdfs_kafka_sync.sh` to compare and stream new lines from the same file.

---

## Logging and Debugging

Configure Snowflake Kafka Connector logs in `SF_connect.properties`:
```properties
logger.snowflake.name = com.snowflake.kafka.connector
logger.snowflake.level = DEBUG
```