# A Web-Based UI for Hadoop Data Pipelines

This project is a comprehensive, web-based toolkit built with Streamlit that provides a user-friendly interface for common data engineering tasks within the Hadoop ecosystem. It allows users to manage data pipelines—from initial table creation in Hive, through incremental data loading, to streaming data into Kafka and finally ingesting it into Snowflake—all without writing code or using complex command-line tools.

The application is designed as a multi-tabbed dashboard, where each tab represents a distinct stage in a typical data pipeline, enabling both technical and non-technical users to manage data flows efficiently.

---

## 🚀 Core Features

The application is organized into a series of logical tabs, each providing a specific set of functionalities:

### 1. Create Hive Tables
-   **Interactive HDFS File Explorer:** A built-in file browser to navigate your HDFS directories.
-   **Multi-File Selection:** Select one or more `.csv` files from various locations in HDFS.
-   **Automatic Schema Inference:** The application reads the header and a sample of the data from each selected CSV to automatically determine column names and data types (e.g., `STRING`, `BIGINT`, `DOUBLE`).
-   **Batch DDL Generation & Execution:** Generates the `CREATE EXTERNAL TABLE` DDL for all selected files and executes them in a single, robust batch operation to create multiple Hive tables at once.

### 2. HDFS Delta to Kafka
-   **Automated Discovery:** Scans a specified HDFS base path for data folders that follow a `..._delta_load` naming convention.
-   **Stateful Incremental Processing:** For each selected folder, it maintains a persistent state to track the last processed date partition.
-   **Delta Streaming:** On subsequent runs, it identifies and streams **only the new data** from new date-partitioned subfolders (e.g., `date=YYYY-MM-DD`).
-   **Dynamic Topic Creation:** Automatically creates and streams data to a corresponding Kafka topic (e.g., folder `tuberculosis_delta_load` streams to topic `tuberculosis_delta_topic`).
-   **Data Enrichment:** Appends the system's current date as a `load_date` column to every message sent to Kafka, providing valuable audit information.

### 3. Hive Delta to Kafka
-   **Automated Partition Discovery:** Scans selected Hive tables to find new partitions based on the `load_date` partition key.
-   **Persistent State Management:** Utilizes a `kafka_migrator_state.json` file to remember the last partition streamed for each table, ensuring data is not re-processed on subsequent runs.
-   **Data Preview:** Allows users to view a sample of the data from the **new partitions only** before committing to the stream.
-   **Concurrent Streaming:** Uses multi-threading to stream data from multiple Hive tables to Kafka simultaneously, significantly improving performance.
-   **Data Enrichment:** Similar to the HDFS pipeline, it appends the current system date as a `load_date` column to every message.

### 4. Kafka to Snowflake
-   **Unified Topic Consumption:** Automatically consolidates the list of Kafka topics generated from both the "HDFS to Kafka" and "Hive to Kafka" pipelines.
-   **Batch Ingestion into Pre-existing Tables:** Designed to load data into tables that are already created in Snowflake.
-   **"Stop-When-Done" Logic:** Intelligently determines the last message offset for each topic partition at the start of the run. The consumer processes all messages up to that point and then gracefully shuts down, making it perfect for batch ETL jobs.
-   **Live Progress Monitoring:** The UI provides real-time feedback on the ingestion progress for each topic partition, showing how many messages have been consumed versus the target.

---

## 🔧 Project Structure and File Purpose

This project is structured with a clear separation between the UI logic (`app.py`) and the backend business logic (individual class modules).

-   `app.py`
    -   **Purpose:** The main entry point for the Streamlit web application.
    -   **Contains:** All UI layout code (tabs, buttons, expanders, logs) and orchestrates calls to the various logic classes. It holds the "view" part of the application.

-   `create_hive_table.py`
    -   **Purpose:** Contains the `HiveTableCreator` class.
    -   **Logic:** Handles all HDFS interactions (listing files, reading CSVs for schema inference) and executes `CREATE TABLE` DDLs in Hive, typically via `beeline` subprocesses.

-   `delta_hdfs_kafka_producer.py`
    -   **Purpose:** Contains the `HdfsToKafkaProducer` class.
    -   **Logic:** Implements the "HDFS Delta to Kafka" pipeline. Manages its own state, discovers new date partitions in HDFS, and streams file contents to Kafka.

-   `hive_delta_kafkaproducer.py` 
    -   **Purpose:** Contains the `KafkaMigrator` class.
    -   **Logic:** Implements the "Hive Delta to Kafka" pipeline. Manages persistent state in a JSON file, discovers new Hive partitions, previews data, and streams table rows to Kafka concurrently.

-   `sf_consumer.py` 
    -   **Purpose:** Contains the `SnowflakeConsumer` class.
    -   **Logic:** Implements the "Kafka to Snowflake" pipeline. Manages the batch ingestion process, determines when all topics are fully consumed, and handles inserting data into Snowflake tables.

-   `config.py` & `SF_connect.properties`
    -   **Purpose:** Configuration files used to store sensitive or environment-specific connection details, such as Snowflake credentials. This is a good practice to separate configuration from code.

-   `load_incremental_data.py`
    -   **Purpose:** The precursor script for the "HDFS to Hive" incremental loading logic. The functionality of this script has been encapsulated into a class and integrated into the Streamlit UI.

-   `scripts.sh`, `hdfs_to_kafka.sh`
    -   **Purpose:** Utility shell scripts, likely used for manual execution of parts of the pipeline or for setup tasks.

-   `structure.txt`, `structure.py`
    -   **Purpose:** Utility files, likely used during development to inspect or define data structures.

---

## ⚙️ Setup and Installation

The machine running this Streamlit app must have the necessary command-line tools installed and available in its `PATH`.

**Prerequisites:**
-   Python 3.8+
-   `hdfs` command-line client
-   `beeline` command-line client (for Hive interaction)

**Installation Steps:**

1.  **Clone the repository or set up the project files.**

2.  **Install Python dependencies from `requirements.txt`:**
    ```bash
    pip install -r requirements.txt
    ```
    *Note: A typical `requirements.txt` would include `streamlit`, `pandas`, `pyhive`, and `confluent-kafka-python`.*

3.  **Configure the Application:**
    -   Open `app.py` and modify the variables in the `--- CONFIGURATION ---` section at the top to match your environment (HDFS host, Hive host, Kafka server, etc.).
    -   Update `config.py` or `SF_connect.properties` with your Snowflake credentials.

## ▶️ How to Run

1.  Navigate to the root directory of the project in your terminal.
    ```bash
    cd /path/to/your/project_directory
    ```
2.  Run the Streamlit application:
    ```bash
    streamlit run app.py
    ```
3.  A new tab should automatically open in your web browser with the application running. You can now start using the DataOps Toolkit.