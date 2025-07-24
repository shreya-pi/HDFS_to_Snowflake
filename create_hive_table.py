# # table_creator.py

# import re
# import pandas as pd
# from hdfs import InsecureClient
# from pyhive import hive
# import subprocess
# import io # Needed to treat string as a file for pandas

# class HiveTableCreator:
#     """
#     A class to manage creating Hive tables from CSV files in HDFS.
#     *** REVISED to use subprocess for all HDFS interactions for consistency. ***
#     """
#     def __init__(self, hdfs_host, hdfs_port, hdfs_user, hive_host, hive_port, hive_user, hive_db):
#         # We no longer need HDFS connection details here, but keep them for consistency
#         self.hive_host = hive_host
#         self.hive_port = hive_port
#         self.hive_user = hive_user
#         self.hive_db = hive_db
#         print("HiveTableCreator initialized (using subprocess for HDFS).")

#     def _get_hdfs_client(self):
#         """Helper to create a new HDFS client instance."""
#         return InsecureClient(f'http://{self.hdfs_host}:{self.hdfs_port}', user=self.hdfs_user)
    
#     def _run_command(self, cmd_list):
#         """Private helper to run shell commands."""
#         try:
#             return subprocess.check_output(cmd_list).decode()
#         except subprocess.CalledProcessError as e:
#             raise RuntimeError(f"HDFS command failed: {' '.join(cmd_list)}\nError: {e.output.decode()}")

#     def _get_hive_connection(self):
#         """Helper to create and return a new Hive connection."""
#         return hive.Connection(
#             host=self.hive_host, port=self.hive_port, username=self.hive_user,
#             database=self.hive_db, auth='NOSASL'
#         )

#     def list_hdfs_path(self, path):
#         """General purpose HDFS lister, using the hdfs command-line tool."""
#         try:
#             # --- KEY CHANGE: Use subprocess instead of the hdfs library ---
#             raw_output = self._run_command(["hdfs", "dfs", "-ls", path])
#             lines = raw_output.strip().splitlines()
            
#             if lines and lines[0].startswith("Found"):
#                 lines = lines[1:]

#             file_details = []
#             for line in lines:
#                 parts = line.split()
#                 if len(parts) < 8: continue
                
#                 # The full path is the last part of the output
#                 full_path = parts[-1]
#                 details = {
#                     'type': 'DIRECTORY' if parts[0].startswith('d') else 'FILE',
#                     'name': full_path.split('/')[-1],
#                     'path': full_path
#                 }
#                 file_details.append(details)
#             return file_details
#         except Exception as e:
#             raise RuntimeError(f"Could not list HDFS path '{path}': {e}")

#     def _clean_column_name(self, col_name):
#         cleaned = re.sub(r'[^0-9a-zA-Z_]', '_', col_name)
#         cleaned = cleaned.strip('_')
#         if cleaned and cleaned[0].isdigit(): cleaned = '_' + cleaned
#         return cleaned.lower()

#     def infer_schema_from_csv(self, hdfs_file_path):
#         """Reads a CSV from HDFS using 'hdfs dfs -cat' and infers its schema."""
#         try:
#             # --- KEY CHANGE: Use 'hdfs dfs -cat' and pipe to pandas ---
#             csv_content = self._run_command(["hdfs", "dfs", "-cat", hdfs_file_path])
#             # Use io.StringIO to treat the string content as a file
#             df = pd.read_csv(io.StringIO(csv_content), nrows=100)
            
#             schema = []
#             for col in df.columns:
#                 pandas_type = str(df[col].dtype)
#                 hive_type = 'STRING'
#                 if 'int' in pandas_type: hive_type = 'BIGINT'
#                 elif 'float' in pandas_type: hive_type = 'DOUBLE'
#                 elif 'bool' in pandas_type: hive_type = 'BOOLEAN'
#                 cleaned_col = self._clean_column_name(col)
#                 schema.append((cleaned_col, hive_type))
#             return schema
#         except Exception as e:
#             raise RuntimeError(f"Error inferring schema from {hdfs_file_path}: {e}")

#     def generate_create_table_ddl(self, table_name, schema, location_path):
#         """Generates the Hive CREATE TABLE DDL string (no changes needed)."""
#         if not schema: return None
#         columns_ddl = ",\n".join([f"  `{col_name}` {col_type}" for col_name, col_type in schema])
#         ddl = f"""
# CREATE EXTERNAL TABLE IF NOT EXISTS `{self.hive_db}`.`{table_name}` (
# {columns_ddl}
# )
# PARTITIONED BY (`load_date` STRING)
# ROW FORMAT DELIMITED
# FIELDS TERMINATED BY ','
# STORED AS TEXTFILE
# LOCATION '{location_path}'
# TBLPROPERTIES ("skip.header.line.count"="1")
# """
#         return ddl

#     def execute_hive_ddl(self, ddl_query):
#         """Executes the DDL query in Hive (no changes needed)."""
#         try:
#             with self._get_hive_connection() as conn:
#                 with conn.cursor() as cursor:
#                     cursor.execute(ddl_query)
#             return "Table created successfully!"
#         except Exception as e:
#             raise RuntimeError(f"An error occurred while executing the Hive query: {e}")






# table_creator.py

import re
import pandas as pd
import subprocess
import io

class HiveTableCreator:
    """
    A class to manage creating Hive tables.
    *** Using subprocess for ALL backend interactions to avoid library conflicts. ***
    """
    def __init__(self, hive_host, hive_port, hive_user, hive_db, **kwargs):
        self.jdbc_url = f"jdbc:hive2://{hive_host}:{hive_port}/{hive_db}"
        self.hive_user = hive_user
        self.hive_db = hive_db
        print("HiveTableCreator initialized (using subprocess for all operations).")

    def _run_command(self, cmd_list):
        """Private helper to run shell commands and handle errors."""
        try:
            return subprocess.check_output(cmd_list, timeout=60).decode()
        except subprocess.CalledProcessError as e:
            raise RuntimeError(f"Command failed: {' '.join(cmd_list)}\nError: {e.output}")
        except subprocess.TimeoutExpired:
            raise RuntimeError(f"Command timed out: {' '.join(cmd_list)}")

    def list_hdfs_path(self, path):
        """Lists HDFS path contents using the 'hdfs dfs -ls' command."""
        try:
            raw_output = self._run_command(["hdfs", "dfs", "-ls", path])
            lines = raw_output.strip().splitlines()
            if lines and lines[0].startswith("Found"):
                lines = lines[1:]
            
            file_details = []
            for line in lines:
                parts = line.split()
                if len(parts) < 8: continue
                full_path = parts[-1]
                details = {
                    'type': 'DIRECTORY' if parts[0].startswith('d') else 'FILE',
                    'name': full_path.split('/')[-1],
                    'path': full_path
                }
                file_details.append(details)
            return file_details
        except Exception as e:
            raise RuntimeError(f"Could not list HDFS path '{path}': {e}")

    def _clean_column_name(self, col_name):
        cleaned = re.sub(r'[^0-9a-zA-Z_]', '_', col_name)
        cleaned = cleaned.strip('_')
        if cleaned and cleaned[0].isdigit(): cleaned = '_' + cleaned
        return cleaned.lower()

    def infer_schema_from_csv(self, hdfs_file_path):
        """Infers schema using 'hdfs dfs -cat' and pandas."""
        try:
            csv_content = self._run_command(["hdfs", "dfs", "-cat", hdfs_file_path])
            df = pd.read_csv(io.StringIO(csv_content), nrows=100)
            
            schema = []
            for col in df.columns:
                pandas_type = str(df[col].dtype)
                hive_type = 'STRING'
                if 'int' in pandas_type: hive_type = 'BIGINT'
                elif 'float' in pandas_type: hive_type = 'DOUBLE'
                elif 'bool' in pandas_type: hive_type = 'BOOLEAN'
                cleaned_col = self._clean_column_name(col)
                schema.append((cleaned_col, hive_type))
            return schema
        except Exception as e:
            raise RuntimeError(f"Error inferring schema from {hdfs_file_path}: {e}")

    def generate_create_table_ddl(self, table_name, schema, location_path):
        """Generates the Hive CREATE TABLE DDL string."""
        if not schema: return None
        columns_ddl = ",\n".join([f"  `{col_name}` {col_type}" for col_name, col_type in schema])
        # IMPORTANT: Add a semicolon at the end for beeline -e
        ddl = f"""
CREATE EXTERNAL TABLE IF NOT EXISTS `{self.hive_db}`.`{table_name}` (
{columns_ddl}
)
PARTITIONED BY (`load_date` STRING)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
LOCATION '{location_path}'
TBLPROPERTIES ("skip.header.line.count"="1");
"""
        return ddl
        
    def execute_hive_ddl(self, ddl_query):
        """Executes a DDL query using the beeline command-line tool."""
        try:
            cmd = ["beeline", "-u", self.jdbc_url, "-n", self.hive_user, "-e", ddl_query]
            self._run_command(cmd)
            return "Table creation command sent to Hive successfully!"
        except Exception as e:
            raise RuntimeError(f"An error occurred while executing the Hive DDL via beeline: {e}")