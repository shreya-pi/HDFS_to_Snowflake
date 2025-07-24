# app.py

import streamlit as st
import threading
import time
import os

# Import the logic classes from our new modules
from load_incremental_data import IncrementalLoader
from hive_delta_kafkaproducer import KafkaMigrator
from sf_consumer import SnowflakeConsumer
from create_hive_table import HiveTableCreator
from delta_hdfs_kafka_producer import HdfsToKafkaProducer
from config import SNOWFLAKE_CONFIG

# ==============================================================================
# CONFIGURATION
# --- Update these values to match your environment ---
# ==============================================================================
HDFS_BASE_PATH = "/user/"
HDFS_HOST = 'localhost'
HDFS_PORT = 9870
HDFS_USER = 'hadoop'

HIVE_DATABASE = "test"
HIVE_HOST = "localhost"
HIVE_PORT = 10000
HIVE_USERNAME = "hadoop"
HIVE_PARTITION_COLUMN = "load_date"
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'

# ==============================================================================
# INITIALIZE LOGIC HANDLERS
# These objects are created once and reused.
# @st.cache_resource is used to prevent re-initialization on every UI interaction.
# ==============================================================================
@st.cache_resource
def get_table_creator():
    return HiveTableCreator(
        hdfs_host=HDFS_HOST, hdfs_port=HDFS_PORT, hdfs_user=HDFS_USER,
        hive_host=HIVE_HOST, hive_port=HIVE_PORT, hive_user=HIVE_USERNAME,
        hive_db=HIVE_DATABASE
    )

@st.cache_resource
def get_loader():
    return IncrementalLoader(
        hdfs_base_path=HDFS_BASE_PATH,
        hive_db=HIVE_DATABASE,
        hive_host=HIVE_HOST,
        hive_port=HIVE_PORT,
        partition_col=HIVE_PARTITION_COLUMN
    )

@st.cache_resource
def get_migrator():
    return KafkaMigrator(
        kafka_brokers=KAFKA_BOOTSTRAP_SERVERS,
        hive_db=HIVE_DATABASE,
        hive_host=HIVE_HOST,
        hive_port=HIVE_PORT,
        hive_user=HIVE_USERNAME
    )

@st.cache_resource
def get_hdfs_to_kafka_producer():
    return HdfsToKafkaProducer(
        kafka_brokers=KAFKA_BOOTSTRAP_SERVERS,
        hdfs_base_path=HDFS_BASE_PATH
    )

@st.cache_resource
def get_snowflake_consumer():
    return SnowflakeConsumer(
        kafka_brokers=KAFKA_BOOTSTRAP_SERVERS, snowflake_config=SNOWFLAKE_CONFIG
    )

# Initialize the logic handlers
# These will be used throughout the app to perform operations.
creator = get_table_creator()
loader = get_loader()
migrator = get_migrator()
hdfs_producer = get_hdfs_to_kafka_producer()
consumer = get_snowflake_consumer()


# Initialize session state variables
if 'delta_folders' not in st.session_state: st.session_state.delta_folders = []
if 'folder_selector' not in st.session_state: st.session_state.folder_selector = []
if 'selected_tables_from_load' not in st.session_state: st.session_state.selected_tables_from_load = []
if 'stop_ingestion' not in st.session_state: st.session_state.stop_ingestion = False
if 'selected_files_list' not in st.session_state: st.session_state.selected_files_list = []
if 'selected_tables_from_hive' not in st.session_state: st.session_state.selected_tables_from_hive = []
if 'selected_tables_from_hdfs' not in st.session_state: st.session_state.selected_tables_from_hdfs = []



# ==============================================================================
# STREAMLIT UI LAYOUT
# ==============================================================================

st.set_page_config(page_title="DataOps Tools", layout="wide")
st.title("DataOps Tools 🛠️")

tab1, tab2, tab3, tab4, tab5 = st.tabs(["Create Hive Table","Incremental Hive Load", "Hive to Kafka Migration", "HDFS to Kafka Streaming", "Kafka to Snowflake Ingestion"])


# --- TAB 1: Create Hive Table from HDFS CSV ---
with tab1:
    st.header("Create Hive Table from HDFS File")
    st.info("Use the explorer to select one or more `.csv` files from different locations. Then, review your selections and create all tables in a single batch operation.")

    col1, col2 = st.columns([1, 1])

    # --- HDFS EXPLORER ---
    with col1:
        st.subheader("HDFS File Explorer")
        with st.container(height=500): # Use a container to make it scrollable
            if 'hdfs_explorer_path' not in st.session_state:
                st.session_state.hdfs_explorer_path = HDFS_BASE_PATH
            
            current_path = st.session_state.hdfs_explorer_path
            st.write(f"**Current Path:** `{current_path}`")

            if current_path != HDFS_BASE_PATH and current_path != '/':
                if st.button("⬆️ Go Up", key="up_level"):
                    st.session_state.hdfs_explorer_path = os.path.dirname(current_path)
                    st.rerun()

            try:
                path_contents = creator.list_hdfs_path(current_path)
                for item in path_contents:
                    if item['type'] == 'DIRECTORY':
                        if st.button(f"🗂️ {item['name']}", key=f"dir_{item['path']}_{item['name']}", use_container_width=True):
                            st.session_state.hdfs_explorer_path = item['path']
                            st.rerun()
                    elif item['name'].endswith('.csv'):
                        # Add a checkbox to select the file
                        is_selected = item['path'] in st.session_state.selected_files_list
                        if st.checkbox(f"📄 {item['name']}", value=is_selected, key=f"check_{item['path']}"):
                            # Add to list if not already there
                            if item['path'] not in st.session_state.selected_files_list:
                                st.session_state.selected_files_list.append(item['path'])
                        else:
                            # Remove from list if unchecked
                            if item['path'] in st.session_state.selected_files_list:
                                st.session_state.selected_files_list.remove(item['path'])
                    else:
                        st.text(f"▫️ {item['name']}")

            except Exception as e:
                st.error(f"Error listing HDFS path: {e}")

    # --- SELECTIONS and BATCH CREATION WORKFLOW ---
    with col2:
        st.subheader("Selected Files for Table Creation")
        
        if not st.session_state.selected_files_list:
            st.info("Select one or more CSV files from the explorer on the left.")
        else:
            st.write("The following files will be used to create Hive tables:")
            # Display the list of selected files
            for f_path in st.session_state.selected_files_list:
                st.markdown(f"- `{f_path}`")

            if st.button("🗑️ Clear All Selections"):
                st.session_state.selected_files_list.clear()
                st.rerun()

            st.divider()

            if st.button("🚀 Create All Selected Tables", type="primary"):
                st.subheader("📜 Batch Creation Log")
                log_placeholder = st.empty()
                full_log = ["Starting batch process..."]
                log_placeholder.code("\n".join(full_log))

                # --- KEY CHANGE: Phase 1 - Generate all DDLs first ---
                ddl_statements = {} # Use a dict to store {table_name: ddl_string}
                with st.spinner("Step 1/2: Inferring schemas and generating DDLs..."):
                    for hdfs_file_path in st.session_state.selected_files_list:
                        table_name = "" # Ensure table_name is in scope for the except block
                        try:
                            file_name_only = hdfs_file_path.split('/')[-1]
                            table_name = os.path.splitext(file_name_only)[0].lower().replace('-', '_')
                            
                            log_line = f"--- Analyzing: {file_name_only} ---"
                            full_log.append(log_line); log_placeholder.code("\n".join(full_log))

                            schema = creator.infer_schema_from_csv(hdfs_file_path)
                            location = os.path.dirname(hdfs_file_path)
                            ddl = creator.generate_create_table_ddl(table_name, schema, location)
                            
                            if ddl:
                                ddl_statements[table_name] = ddl
                                log_line = f"[{table_name}] ✅ DDL successfully generated."
                                full_log.append(log_line); log_placeholder.code("\n".join(full_log))
                            else:
                                raise ValueError("DDL generation returned None.")
                                
                        except Exception as e:
                            log_line = f"[{table_name}] ❌ FAILED during analysis: {e}"
                            full_log.append(log_line); log_placeholder.code("\n".join(full_log))
                
                full_log.append("\n--- DDL Generation Complete ---")
                log_placeholder.code("\n".join(full_log))

                # --- KEY CHANGE: Phase 2 - Execute all DDLs in a single session ---
                created_tables = []
                if ddl_statements:
                    with st.spinner("Step 2/2: Opening Hive connection and executing all DDLs..."):
                        try:
                            # Use the creator's private method to get ONE connection for the whole batch
                            with creator._get_hive_connection() as conn:
                                with conn.cursor() as cursor:
                                    full_log.append("\n[Hive Session] Connection opened.")
                                    log_placeholder.code("\n".join(full_log))
                                    
                                    for table_name, ddl in ddl_statements.items():
                                        try:
                                            log_line = f"[Hive Session] Executing DDL for `{table_name}`..."
                                            full_log.append(log_line); log_placeholder.code("\n".join(full_log))
                                            cursor.execute(ddl)
                                            created_tables.append(table_name)
                                        except Exception as e:
                                            log_line = f"[Hive Session] ❌ FAILED to execute DDL for `{table_name}`: {e}"
                                            full_log.append(log_line); log_placeholder.code("\n".join(full_log))
                                            # Continue to the next DDL even if one fails
                                            
                                    full_log.append("[Hive Session] All tasks sent. Closing connection.")
                                    log_placeholder.code("\n".join(full_log))
                        except Exception as e:
                            full_log.append(f"❌ FAILED to connect to Hive for batch execution: {e}")
                            log_placeholder.code("\n".join(full_log))

                st.success("Batch creation process finished!")
                if created_tables:
                    # Pass the successfully created tables to other tabs
                    st.session_state.selected_tables_from_hive = created_tables

                # full_log = []
                # created_tables = []

                # with st.spinner("Processing all selected files..."):
                #     for hdfs_file_path in st.session_state.selected_files_list:
                #         try:
                #             file_name_only = hdfs_file_path.split('/')[-1]
                #             table_name = os.path.splitext(file_name_only)[0].lower().replace('-', '_')
                            
                #             log_line = f"--- Processing: {file_name_only} ---"
                #             full_log.append(log_line); log_placeholder.code("\n".join(full_log))

                #             # 1. Infer Schema
                #             log_line = f"[{table_name}] Inferring schema..."
                #             full_log.append(log_line); log_placeholder.code("\n".join(full_log))
                #             schema = creator.infer_schema_from_csv(hdfs_file_path)
                            
                #             # 2. Generate DDL
                #             location = os.path.dirname(hdfs_file_path)
                #             ddl = creator.generate_create_table_ddl(table_name, schema, location)
                #             log_line = f"[{table_name}] DDL generated."
                #             full_log.append(log_line); log_placeholder.code("\n".join(full_log))

                #             # 3. Execute DDL
                #             log_line = f"[{table_name}] Executing DDL in Hive..."
                #             full_log.append(log_line); log_placeholder.code("\n".join(full_log))
                #             result = creator.execute_hive_ddl(ddl)
                            
                #             log_line = f"[{table_name}] ✅ {result}"
                #             full_log.append(log_line); log_placeholder.code("\n".join(full_log))
                #             created_tables.append(table_name)
                            
                #         except Exception as e:
                #             log_line = f"[{table_name}] ❌ FAILED: {e}"
                #             full_log.append(log_line); log_placeholder.code("\n".join(full_log))
                #             continue # Move to the next file on failure
                
                # st.success("Batch creation process finished!")
                # if created_tables:
                #     st.session_state.selected_tables_from_load = created_tables




    # # Initialize session state for multi-file workflow
    # if 'selected_files' not in st.session_state:
    #     st.session_state.selected_files = []
    # if 'generated_ddls' not in st.session_state:
    #     st.session_state.generated_ddls = {} # Use a dict to store DDLs by table name

    # # --- HDFS EXPLORER (repurposed for file selection) ---
    # with st.expander("📁 HDFS File Explorer", expanded=True):
    #     if 'hdfs_explorer_path' not in st.session_state:
    #         st.session_state.hdfs_explorer_path = HDFS_BASE_PATH
        
    #     current_path = st.session_state.hdfs_explorer_path
    #     st.write(f"**Current Path:** `{current_path}`")

    #     if current_path != HDFS_BASE_PATH:
    #         if st.button("⬆️ Go Up One Level"):
    #             st.session_state.hdfs_explorer_path = os.path.dirname(current_path)
    #             st.rerun()

    #     try:
    #         path_contents = creator.list_hdfs_path(current_path)
    #         for item in path_contents:
    #             if item['type'] == 'DIRECTORY':
    #                 if st.button(f"🗂️ {item['name']}", key=f"dir_{item['path']}"):
    #                     st.session_state.hdfs_explorer_path = item['path']
    #                     st.rerun()
    #             elif item['name'].endswith('.csv'):
    #                 if st.button(f"📄 {item['name']}", key=f"file_{item['path']}"):
    #                     st.session_state.selected_file_path = item['path']
    #                     st.success(f"Selected file: `{item['path']}`")
    #             else:
    #                 # Non-csv file, not selectable
    #                 st.write(f"▫️ {item['name']}")

    #     except Exception as e:
    #         st.error(f"Error listing HDFS path: {e}")

    # # --- TABLE CREATION WORKFLOW ---
    # if st.session_state.selected_file_path:
    #     st.divider()
    #     st.subheader("Schema Inference and Table Creation")
        
    #     selected_file = st.session_state.selected_file_path
    #     file_name_only = selected_file.split('/')[-1]
        
    #     # Suggest a table name based on the CSV file name
    #     default_table_name = os.path.splitext(file_name_only)[0].lower().replace('-', '_')
    #     table_name_input = st.text_input("Enter a name for the new Hive table:", value=default_table_name)

    #     if st.button("📝 Infer Schema & Generate DDL"):
    #         if not table_name_input:
    #             st.warning("Please provide a table name.")
    #         else:
    #             with st.spinner(f"Reading `{file_name_only}` and inferring schema..."):
    #                 try:
    #                     schema = creator.infer_schema_from_csv(selected_file)
    #                     st.session_state.generated_schema = schema
                        
    #                     # The location should be a directory, not the file itself
    #                     location = os.path.dirname(selected_file)
    #                     ddl = creator.generate_create_table_ddl(table_name_input, schema, location)
    #                     st.session_state.generated_ddl = ddl
                        
    #                     st.success("Schema inferred successfully!")
    #                     st.write("Generated DDL:")
    #                     st.code(ddl, language='sql')

    #                 except Exception as e:
    #                     st.error(f"Failed to infer schema: {e}")
        
    #     if 'generated_ddl' in st.session_state and st.session_state.generated_ddl:
    #         if st.button("🚀 Create Table in Hive", type="primary"):
    #             with st.spinner(f"Executing DDL to create table `{table_name_input}`..."):
    #                 try:
    #                     result = creator.execute_hive_ddl(st.session_state.generated_ddl)
    #                     st.success(result)
    #                     # Clean up state to prevent re-submission
    #                     del st.session_state.generated_ddl
    #                     del st.session_state.generated_schema
    #                 except Exception as e:
    #                     st.error(f"Failed to create table: {e}")


# --- TAB 2: Incremental Hive Load ---
with tab2:
    st.header("Incremental Hive Load")
    st.info("Scans HDFS for new date-partitioned data and adds it to the corresponding Hive table.")

    st.subheader("Incremental Load Workflow")
    if st.button("Scan for Delta Folders", key="scan_delta"):
        with st.spinner("Scanning HDFS..."):
            try:
                st.session_state.delta_folders = loader.discover_delta_folders()
            except Exception as e:
                st.error(f"Failed to scan HDFS: {e}")
                st.session_state.delta_folders = []

    if st.session_state.delta_folders:
        folder_names = [path.split('/')[-1] for path in st.session_state.delta_folders]
        selected_folders = st.multiselect(
            "Select folders/tables to process:",
            options=folder_names,
            key="folder_selector"
        )

                # --- KEY CHANGE 2: Infer table names whenever the selection changes ---
        # This logic runs automatically as the user interacts with the multiselect.
        if st.session_state.folder_selector:
            inferred_tables = [name.replace("_delta_load", "") for name in st.session_state.folder_selector]
            st.session_state.selected_tables_from_load = inferred_tables
            st.success(f"**Tables selected for Step 2:** `{', '.join(inferred_tables)}`")
        else:
            # Clear the state if nothing is selected
            st.session_state.selected_tables_from_load = []

        if st.button("▶️ Run Incremental Load", disabled=not selected_folders, key="run_load"):
            st.subheader("📜 Live Log")
            log_placeholder = st.empty()
            full_log = []
            paths_to_process = [path for path in st.session_state.delta_folders if path.split('/')[-1] in selected_folders]

            for path in paths_to_process:
                # Call the method from our loader instance
                for log_line in loader.process_table(path):
                    full_log.append(log_line)
                    log_placeholder.code("\n".join(full_log))
            
            st.success("All selected incremental load tasks are complete!")

    st.divider()

    # if 'delta_folders' not in st.session_state:
    #     st.session_state.delta_folders = []
    # --- HDFS EXPLORER UI ---
    with st.expander("📁 HDFS Explorer", expanded=False):
        # Initialize the explorer's current path in session state
        if 'hdfs_explorer_path' not in st.session_state:
            st.session_state.hdfs_explorer_path = HDFS_BASE_PATH

        current_path = st.session_state.hdfs_explorer_path
        st.write(f"**Current Path:** `{current_path}`")

        # "Go Up" button logic
        if current_path != HDFS_BASE_PATH:
            if st.button("⬆️ Go Up One Level"):
                # Navigate to the parent directory
                st.session_state.hdfs_explorer_path = os.path.dirname(current_path)
                st.rerun() # Force an immediate rerun to update the view

        # Display the contents of the current path
        try:
            with st.spinner(f"Loading contents of {current_path}..."):
                path_contents = loader.list_hdfs_path(current_path)
            
            if path_contents:
                display_data = []
                for item in path_contents:
                    # Prepend an icon and make the name clickable if it's a directory
                    name_display = f"🗂️ {item['name']}" if item['type'] == 'Directory' else f"📄 {item['name']}"
                    display_data.append({
                        "name": name_display,
                        "size": item['size'],
                        "modified": item['modified'],
                        "owner": item['owner'],
                        "type": item['type'],
                        "path": item['path'] # Keep the full path hidden but available
                    })

                # Using st.data_editor to get row selection events
                edited_df = st.data_editor(
                    display_data,
                    column_config={
                        # Make the 'name' column a "clickable" text column
                        "name": st.column_config.TextColumn("Name", help="Click a directory to navigate into it."),
                        # Hide the 'type' and 'path' columns from the user, but keep them in the data
                        "type": None,
                        "path": None,
                    },
                    hide_index=True,
                    key=f"explorer_{current_path}", # A unique key to avoid state issues
                    on_change=None # We will handle clicks below
                )

                # Check if a row was "clicked" (selected) by the user
                if st.session_state[f"explorer_{current_path}"].get("selection", {}).get("rows"):
                    # Get the index of the selected row
                    selected_row_index = st.session_state[f"explorer_{current_path}"]["selection"]["rows"][0]
                    selected_item = display_data[selected_row_index]

                    # If the selected item is a directory, navigate into it
                    if selected_item['type'] == 'Directory':
                        st.session_state.hdfs_explorer_path = selected_item['path']
                        st.rerun()

            else:
                st.info("This directory is empty.")

            #     # Use columns for a cleaner layout
            #     col1, col2, col3, col4 = st.columns([4, 1, 2, 2])
            #     col1.write("**Name**")
            #     col2.write("**Size**")
            #     col3.write("**Modified**")
            #     col4.write("**Owner**")
            #     st.divider()

            #     for item in path_contents:
            #         col1, col2, col3, col4 = st.columns([4, 1, 2, 2])
            #         # If it's a directory, make the name a clickable button
            #         if item['type'] == 'Directory':
            #             if col1.button(f"🗂️ {item['name']}", key=f"dir_{item['path']}", use_container_width=True):
            #                 st.session_state.hdfs_explorer_path = item['path']
            #                 st.rerun()
            #         else:
            #             col1.write(f"📄 {item['name']}")
                    
            #         col2.write(item['size'])
            #         col3.write(item['modified'])
            #         col4.write(item['owner'])
            # else:
            #     st.info("This directory is empty.")

        except Exception as e:
            st.error(str(e))

    st.divider()


# --- TAB 3: Hive to Kafka Migration ---
with tab3:
    st.header("Hive to Kafka (Incremental Stream)")
    st.info(
        "This tool finds new partitions in the selected Hive tables (based on `load_date`) "
        "and allows you to preview the new data before streaming it to Kafka."
    )

    # Initialize state for this tab
    if 'partitions_to_process' not in st.session_state:
        st.session_state.partitions_to_process = {}

    if not st.session_state.selected_tables_from_load:
        st.warning("Please go to **Step 1** and select at least one folder. The corresponding tables will appear here.")
    else:
        st.session_state.selected_tables_from_hive= st.multiselect(
            "Tables targeted for incremental streaming:",
            options=st.session_state.selected_tables_from_load,
            default=st.session_state.selected_tables_from_load,
            disabled=True
        )

        st.subheader("1. Discover New Partitions")
        if st.button("🔍 Check for New Data", key="discover_partitions"):
            st.session_state.partitions_to_process.clear()
            log_placeholder_discover = st.empty()
            full_log_discover = []
            
            with st.spinner("Querying Hive for new partitions..."):
                for table in st.session_state.selected_tables_from_hive:
                    new_partitions = []
                    for log_line in migrator.discover_new_partitions(table):
                        if isinstance(log_line, list):
                            new_partitions = log_line
                        else:
                            full_log_discover.append(log_line)
                            log_placeholder_discover.code("\n".join(full_log_discover))
                    st.session_state.partitions_to_process[table] = new_partitions
            st.success("Discovery complete!")

        if st.session_state.partitions_to_process:
            st.subheader("2. Preview and Stream New Data")
            
            has_new_data = any(st.session_state.partitions_to_process.values())
            
            if not has_new_data:
                st.info("✅ All tables are up-to-date. No new data to stream.")
            else:
                # --- KEY UI CHANGE: Iterate and create a section for each table ---
                for table, partitions in st.session_state.partitions_to_process.items():
                    if not partitions:
                        continue # Skip tables with no new data
                    
                    # Use an expander to keep the UI tidy
                    with st.expander(f"**Table: `{table}`** ({len(partitions)} new partition(s) found)"):
                        st.write("Partitions to be streamed:", ", ".join(partitions))

                        # Create a unique key for the preview button
                        if st.button(f"👁️ Preview New Data for `{table}`", key=f"preview_{table}"):
                            with st.spinner(f"Fetching sample data for {table}..."):
                                preview_df = migrator.get_partition_preview(table)
                                if not preview_df.empty:
                                    st.dataframe(preview_df)
                                else:
                                    st.warning("Could not fetch a preview or no data in new partitions.")
                
                st.divider() # Visual separator
                
                st.subheader("3. Execute Stream")
                if st.button("▶️ Stream All New Data to Kafka", key="run_delta_stream"):
                    log_placeholder_stream = st.empty()
                    full_log_stream = []
                    
                    with st.spinner("Streaming new data..."):
                        for log_line in migrator.run_streaming_in_parallel(st.session_state.partitions_to_process):
                            full_log_stream.append(log_line)
                            log_placeholder_stream.code("\n".join(full_log_stream))

                        # for table, partitions in st.session_state.partitions_to_process.items():
                        #     if partitions:
                        #         for log_line in migrator.stream_delta_to_kafka(table, partitions):
                        #             full_log_stream.append(log_line)
                        #             log_placeholder_stream.code("\n".join(full_log_stream))
                    
                    st.success("All new data has been streamed to Kafka.")

# --- TAB 4: HDFS to Kafka Streaming ---
with tab4:
    st.header("HDFS Delta to Kafka Producer")
    st.info(
        "This tool scans HDFS for folders ending in `_delta_load`, finds new date partitions "
        "within them, and streams the raw data directly to a corresponding Kafka topic."
    )

    if 'delta_folders_for_kafka' not in st.session_state:
        st.session_state.delta_folders_for_kafka = []

    if st.button("Scan for HDFS Delta Folders", key="scan_hdfs_delta"):
        with st.spinner("Scanning HDFS..."):
            try:
                st.session_state.delta_folders_for_kafka = hdfs_producer.discover_delta_folders()
            except Exception as e:
                st.error(f"Failed to scan HDFS: {e}")
                st.session_state.delta_folders_for_kafka = []

    if st.session_state.delta_folders_for_kafka:
        folder_names = [path.split('/')[-1] for path in st.session_state.delta_folders_for_kafka]
        
        st.session_state.selected_tables_from_hdfs = st.multiselect(
            "Select delta folders to stream from:",
            options=folder_names,
            key="hdfs_kafka_selector"
        )

        if st.session_state.selected_tables_from_hdfs:
            st.subheader("Batch Streaming Workflow")
            if st.button("▶️ Stream All New Data to Kafka", type="primary", key="run_hdfs_kafka"):
                st.subheader("📜 Live Log")
                log_placeholder = st.empty()
                full_log = []
                
                paths_to_process = [
                    path for path in st.session_state.delta_folders_for_kafka 
                    if path.split('/')[-1] in st.session_state.selected_tables_from_hdfs
                ]

                with st.spinner("Processing all selected folders..."):
                    for hdfs_path in paths_to_process:
                        for log_line in hdfs_producer.stream_folder_to_kafka(hdfs_path):
                            full_log.append(log_line)
                            log_placeholder.code("\n".join(full_log))
                
                st.success("All HDFS to Kafka streaming tasks are complete!")



with tab5:
    st.header("Kafka to Snowflake Batch Ingestion")
    st.info(
        "This tool ingests all current data from the selected Kafka topics into "
        "**pre-existing** Snowflake tables. It will stop automatically when all messages are processed."
    )

    # Check if topics have been selected from Step 1
    if not st.session_state.selected_tables_from_hive and not st.session_state.selected_tables_from_hdfs:
        st.warning("Please go to **Step 3** or **Step 4** and select at least one folder. The corresponding Kafka topics will appear here.")
    else:

        topics_from_hive = [f"{table}_delta_hive_topic" for table in st.session_state.selected_tables_from_hive]

        topics_from_hdfs = []
        for folder_name in st.session_state.selected_tables_from_hdfs:
            table_name = folder_name.replace("_delta_load", "")
            topics_from_hdfs.append(f"{table_name}_delta_hdfs_topic")

        # Derive topic names from the tables selected in Step 1
        # topics_to_process = [f"{table}_topic" for table in st.session_state.selected_tables_from_load]
        
        # Combine and get unique topics using a set
        all_topics_to_process = sorted(list(set(topics_from_hive + topics_from_hdfs)))
    
        if not all_topics_to_process:
            st.warning("Go to **Tab 2** or **Tab 3** to select sources for Kafka topics.")
        else:
            st.success("Topics selected from both 'HDFS to Kafka' and 'Hive to Kafka' tabs will be processed.")
        st.multiselect(
            "Final list of Kafka topics to be consumed:", 
            options=all_topics_to_process, 
            default=all_topics_to_process, 
            disabled=False
        )

        st.subheader("Run Ingestion")
        
        # The main button to kick off the entire process
        if st.button("▶️ Ingest All Data (Stop when done)", key="run_batch_ingest"):
            st.info("Starting BATCH ingestion. The process will monitor Kafka topics and stop automatically when all messages are consumed.")
            log_placeholder_p2 = st.empty()
            full_log_p2 = ["Starting batch ingestion orchestrator..."]
            
            # Use a threading event to allow for manual stopping if needed (optional but good practice)
            stop_event = threading.Event()
            
            # The run_batch_ingestion method is now the single entry point.
            # It yields logs that we can display in real-time.
            for log_line in consumer.run_batch_ingestion(all_topics_to_process, stop_event):
                full_log_p2.append(log_line)
                log_placeholder_p2.code("\n".join(full_log_p2))
            
            st.success("✅ Batch ingestion process has completed.")



