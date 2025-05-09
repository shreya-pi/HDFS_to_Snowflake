# Start zookeeper
$ kafka/bin/zookeeper-server-start.sh kafka/config/zookeeper.properties


# Start kafka server
$ kafka/bin/kafka-server-start.sh kafka/config/server.properties


# 📦 Step 2: Create a Kafka Topic
# Before starting the connector, create a Kafka topic to which you’ll publish the data:
kafka-topics.sh --create --topic topic_name --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1



# To stream data FROM HDFS file to kafka topic
hdfs dfs -cat /path_to_file/file_name.csv | kafka-console-producer.sh --broker-list localhost:9092 --topic topic_name



# 📤 Step 5: Verify Data in Kafka
# To check the messages pushed to the topic:

kafka-console-consumer.sh --bootstrap-server localhost:9092 \
    --topic topic_name --from-beginning
# You should see each line of your CSV streamed into the topic


# Publishing on Snowflake via snowflake-kafka connector
$ bin/connect-standalone.sh config/connect-standalone.properties config/SF_connect.properties

# SF_connect.properties:- contains configuration details for the Snowflake Kafka Connector
# The connector will read from the Kafka topic and write to Snowflake


# make sure target table is in the format
create or replace TABLE DB_NAME.SCHEMA_NAME.TABLE_NAME (
	RECORD_METADATA VARIANT,
           RECORD_CONTENT VARIANT
);





# ----------------------------------------------------------
# For Delta Load from Kafka to Snowflake

#Using Date partitions
Run ~/delta_hdfs_kafka_connector.py


# HDFS sync  script, that compares and streams new lines(keeps counts of linr for same file, does not use date partitions)
 ~/hdfs_kafka_sync.sh
