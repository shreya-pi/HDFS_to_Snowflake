# incremental_loader.py

import subprocess
import datetime

class IncrementalLoader:
    """
    A class to manage the incremental loading of date-partitioned data
    from HDFS into Hive tables.
    """
    def __init__(self, hdfs_base_path, hive_db, hive_host, hive_port, partition_col):
        self.hdfs_base_path = hdfs_base_path
        self.hive_db = hive_db
        self.hive_host = hive_host
        self.hive_port = hive_port
        self.partition_col = partition_col
        print("IncrementalLoader initialized.")

    def _run_command(self, cmd_list):
        """Private helper to run shell commands."""
        try:
            output = subprocess.check_output(cmd_list, stderr=subprocess.STDOUT)
            return output.decode()
        except subprocess.CalledProcessError as e:
            raise subprocess.CalledProcessError(e.returncode, e.cmd, e.output.decode())

    def _run_hive_query_interactive(self, query):
        """A generator to execute a Hive query and yield its output."""
        connection_string = f"jdbc:hive2://{self.hive_host}:{self.hive_port}/{self.hive_db}"
        cmd = ["beeline", "-u", connection_string, "-e", query]
        process = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, bufsize=1)
        for line in iter(process.stdout.readline, ''):
            yield line
        process.wait()
        if process.returncode != 0:
            yield f"\n❌ ERROR: Beeline process finished with exit code {process.returncode}."

    def list_hdfs_path(self, path):
        """
        Lists the contents (files and directories) of a given HDFS path.
        Returns a list of dictionaries with details.
        """
        import pandas as pd
        
        try:
            # The -h flag makes file sizes human-readable (e.g., 1K, 2M)
            raw_output = self._run_command(["hdfs", "dfs", "-ls", "-h", path])
            lines = raw_output.strip().splitlines()
            
            # Skip the "Found X items" header line
            if lines and lines[0].startswith("Found"):
                lines = lines[1:]

            file_details = []
            for line in lines:
                parts = line.split()
                if len(parts) < 8:
                    continue
                
                # HDFS ls output format: [permissions, replicas, owner, group, size, mod_date, mod_time, path]
                details = {
                    'permissions': parts[0],
                    'type': 'Directory' if parts[0].startswith('d') else 'File',
                    'owner': parts[2],
                    'group': parts[3],
                    'size': parts[4],
                    'modified': f"{parts[5]} {parts[6]}",
                    'path': parts[7],
                    'name': parts[7].split('/')[-1] # Just the file/folder name
                }
                file_details.append(details)
            
            return file_details
        except Exception as e:
            # Raise a specific error that the UI can catch
            raise RuntimeError(f"Could not list HDFS path '{path}': {e}")


    def discover_delta_folders(self):
        """Scans HDFS for folders ending in '_delta_load'."""
        try:
            cmd = ["hdfs", "dfs", "-ls", "-C", self.hdfs_base_path]
            all_items = self._run_command(cmd).strip().splitlines()
            delta_folders = sorted([item for item in all_items if item.endswith("_delta_load")])
            return delta_folders
        except Exception as e:
            # We raise the exception so the UI can handle it
            raise RuntimeError(f"Failed to scan HDFS path {self.hdfs_base_path}:\n{e}")

    def process_table(self, delta_folder_path):
        """
        Generator function to process one table, yielding log messages.
        """
        folder_name = delta_folder_path.split('/')[-1]
        table_name = folder_name.replace("_delta_load", "")
        state_file = f"{delta_folder_path}/_state/last_date.txt"
        log_prefix = f"[{table_name}]"
        
        yield f"{log_prefix} --- Starting processing ---"

        # Read last processed date
        last_processed_date = None
        try:
            self._run_command(["hdfs", "dfs", "-mkdir", "-p", f"{delta_folder_path}/_state"])
            txt = self._run_command(["hdfs", "dfs", "-cat", state_file]).strip()
            last_processed_date = datetime.date.fromisoformat(txt)
            yield f"{log_prefix} 📖 Last processed date: {last_processed_date}"
        except subprocess.CalledProcessError as e:
            if "No such file or directory" in str(e.output):
                yield f"{log_prefix} 📖 State file not found. Will process all."
            else:
                yield f"{log_prefix} ❌ Error reading state file: {e.output}. Skipping."
                return

        start_date = (last_processed_date + datetime.timedelta(days=1)) if last_processed_date else None

        # Find and filter date partitions
        try:
            all_partition_paths = self._run_command(["hdfs", "dfs", "-ls", "-C", delta_folder_path]).strip().splitlines()
            all_dates = []
            for path in all_partition_paths:
                if "date=" in path:
                    try:
                        all_dates.append(datetime.date.fromisoformat(path.split("date=")[-1]))
                    except (ValueError, IndexError):
                        yield f"{log_prefix} ⚠️ Skipping malformed directory: {path}"
            
            dates_to_process = sorted([d for d in all_dates if not start_date or d >= start_date])
            
            if not dates_to_process:
                yield f"{log_prefix} ✅ All up to date. No new partitions found."
                return
            yield f"{log_prefix} 🗓️ Found {len(dates_to_process)} new partition(s)."

        except Exception as e:
            yield f"{log_prefix} ❌ Error listing partitions: {e}. Skipping."
            return

        # Process each new partition
        for current_date in dates_to_process:
            partition_value = current_date.isoformat()
            hdfs_partition_location = f"{delta_folder_path}/date={partition_value}"
            fully_qualified_table = f"{self.hive_db}.{table_name}"
            query = (
                f"ALTER TABLE {fully_qualified_table} "
                f"ADD IF NOT EXISTS PARTITION ({self.partition_col}='{partition_value}') "
                f"LOCATION '{hdfs_partition_location}';"
            )
            
            yield f"{log_prefix} 🐝 Executing Hive query for {partition_value}..."
            # Consume the generator to wait for the command to finish
            for _ in self._run_hive_query_interactive(query):
                pass
            yield f"{log_prefix}    ...Done."

            # Update state file
            try:
                write_cmd = ["hdfs", "dfs", "-put", "-f", "-", state_file]
                p = subprocess.Popen(write_cmd, stdin=subprocess.PIPE, text=True)
                p.communicate(input=partition_value)
                yield f"{log_prefix} 🖋️  Updated state file to {partition_value}."
            except Exception as e:
                yield f"{log_prefix} ❌ FAILED to update state file: {e}. Halting."
                break
        
        yield f"{log_prefix} --- Finished processing ---"