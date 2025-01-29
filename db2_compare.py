#!/usr/bin/env python3
# to run, use
# python db2_compare.py --config config.json --output-dir comparison_results

import os
import sys
import argparse
import logging
import json
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Tuple, Any
from dataclasses import dataclass

def setup_db2_environment():
    """Setup DB2 environment variables and paths"""
    db2_home = r"C:\Program Files\IBM\SQLLIB"
    
    # Set DB2 environment variables
    os.environ['DB2_HOME'] = db2_home
    os.environ['DB2PATH'] = db2_home
    
    # Add necessary paths
    bin_path = os.path.join(db2_home, 'BIN')
    lib_path = os.path.join(db2_home, 'LIB')
    python_path = os.path.join(db2_home, 'PYTHON')
    
    # Use user's home directory instead of Python installation directory
    user_home = os.path.expanduser('~')
    clidriver_path = os.path.join(user_home, '.db2', 'clidriver')
    clidriver_bin = os.path.join(clidriver_path, 'bin')
    clidriver_lib = os.path.join(clidriver_path, 'lib')
    
    # Ensure clidriver directories exist
    try:
        os.makedirs(clidriver_bin, exist_ok=True)
        os.makedirs(clidriver_lib, exist_ok=True)
    except Exception as e:
        print(f"Warning: Could not create clidriver directories: {str(e)}")
        # Continue execution even if directory creation fails
    
    # Copy necessary DLLs to clidriver directory
    import shutil
    dlls_to_copy = [
        'db2app64.dll',
        'db2cli64.dll',
        'db2osse64.dll',
        'db2cli.dll',
        'db2app.dll'
    ]
    
    for dll in dlls_to_copy:
        source_path = os.path.join(bin_path, dll)
        if os.path.exists(source_path):
            try:
                target_path = os.path.join(clidriver_bin, dll)
                if not os.path.exists(target_path):
                    shutil.copy2(source_path, clidriver_bin)
                    print(f"Copied {dll} to clidriver/bin")
            except Exception as e:
                print(f"Warning: Failed to copy {dll}: {str(e)}")
    
    paths_to_check = [
        bin_path,
        lib_path,
        python_path,
        clidriver_bin,
        clidriver_lib
    ]
    
    # Ensure paths exist in PATH
    current_path = os.environ.get('PATH', '').split(os.pathsep)
    for path in paths_to_check:
        if os.path.exists(path) and path not in current_path:
            os.environ['PATH'] = path + os.pathsep + os.environ['PATH']
            print(f"Added to PATH: {path}")
    
    # Set additional DB2 environment variables
    os.environ['IBM_DB_HOME'] = db2_home
    os.environ['IBM_DB_DIR'] = db2_home
    os.environ['IBM_DB_LIB'] = lib_path
    os.environ['DB2_CLI_DRIVER_INSTALL_PATH'] = db2_home
    os.environ['IBM_DB_INCLUDE'] = os.path.join(db2_home, 'include')
    os.environ['CLI_DRIVER_INSTALL_PATH'] = clidriver_path
    
    print(f"\nEnvironment Setup:")
    print(f"DB2_HOME: {os.environ.get('DB2_HOME')}")
    print(f"IBM_DB_HOME: {os.environ.get('IBM_DB_HOME')}")
    print(f"CLI_DRIVER_INSTALL_PATH: {os.environ.get('CLI_DRIVER_INSTALL_PATH')}")
    
    # Verify DLL locations
    print("\nDLL Status:")
    for dll in dlls_to_copy:
        locations = []
        for path in paths_to_check:
            dll_path = os.path.join(path, dll)
            if os.path.exists(dll_path):
                locations.append(dll_path)
        
        if locations:
            print(f"Found {dll} in:")
            for loc in locations:
                print(f"  {loc}")
        else:
            print(f"Missing: {dll}")

# Setup DB2 environment before importing ibm_db
setup_db2_environment()

# Load DB2 DLLs
try:
    import ctypes
    dll_path = os.path.join(os.environ.get('DB2_HOME'), 'bin')
    ctypes.CDLL(os.path.join(dll_path, 'db2app64.dll'))
    ctypes.CDLL(os.path.join(dll_path, 'db2cli64.dll'))
    print("Successfully loaded DB2 DLLs")
except Exception as e:
    print(f"Failed to load DB2 DLLs: {str(e)}")
    sys.exit(1)

# Import ibm_db
try:
    print("\nAttempting to import ibm_db...")
    import ibm_db
    print("Successfully imported ibm_db")
except ImportError as e:
    print("\nError: Failed to import ibm_db module.")
    print("Please ensure you have:")
    print("1. Installed IBM DB2 client")
    print("2. Installed ibm_db package using: pip install ibm-db")
    print("3. Added DB2 client bin directory to PATH")
    
    print("\nTrying to reinstall ibm_db...")
    import subprocess
    try:
        subprocess.run([sys.executable, "-m", "pip", "uninstall", "-y", "ibm_db"], check=True)
        subprocess.run([sys.executable, "-m", "pip", "install", "--no-cache-dir", "ibm_db"], check=True)
        print("Reinstallation complete. Please run the script again.")
    except subprocess.CalledProcessError as e:
        print(f"Failed to reinstall: {str(e)}")
    sys.exit(1)

@dataclass
class DbConfig:
    host: str
    port: int
    database: str
    username: str
    password: str

    def get_connection_string(self) -> str:
        return f"DATABASE={self.database};HOSTNAME={self.host};PORT={self.port};PROTOCOL=TCPIP;UID={self.username};PWD={self.password}"

class Db2Comparator:
    def __init__(self, baseline_config: DbConfig, modified_config: DbConfig, output_dir: str):
        self.baseline_config = baseline_config
        self.modified_config = modified_config
        self.output_dir = Path(output_dir)
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        # Setup logging
        self._setup_logging()
        
        # Initialize connections
        self.baseline_conn = None
        self.modified_conn = None

    def _setup_logging(self):
        """Configure logging for the application"""
        logging.basicConfig(
            level=logging.INFO,
            format='%(asctime)s - %(levelname)s - %(message)s',
            handlers=[
                logging.FileHandler(self.output_dir / 'comparison.log', encoding='utf-8'),
                logging.StreamHandler()
            ]
        )
        self.logger = logging.getLogger(__name__)

    def connect(self):
        """Establish connections to both databases"""
        try:
            self.baseline_conn = ibm_db.connect(
                self.baseline_config.get_connection_string(), "", "")
            self.modified_conn = ibm_db.connect(
                self.modified_config.get_connection_string(), "", "")
            self.logger.info("Successfully connected to both databases")
        except Exception as e:
            self.logger.error(f"Failed to connect to databases: {str(e)}")
            raise

    def close(self):
        """Close database connections"""
        if self.baseline_conn:
            ibm_db.close(self.baseline_conn)
        if self.modified_conn:
            ibm_db.close(self.modified_conn)

    def _fetch_all(self, connection, query: str) -> List[Dict]:
        """Execute query and fetch all results as dictionaries"""
        stmt = ibm_db.exec_immediate(connection, query)
        result = []
        dictionary = ibm_db.fetch_assoc(stmt)
        while dictionary:
            result.append({k.lower(): v for k, v in dictionary.items()})
            dictionary = ibm_db.fetch_assoc(stmt)
        return result

    def compare_tables(self):
        """Compare table structures between databases"""
        self.logger.info("Starting table comparison")
        
        table_query = """
        SELECT t.TABSCHEMA, t.TABNAME, t.TYPE, t.CREATE_TIME,
               c.COLNAME, c.TYPENAME, c.LENGTH, c.SCALE, c.NULLS, c.DEFAULT,
               c.COLNO, c.IDENTITY, c.GENERATED
        FROM SYSCAT.TABLES t
        JOIN SYSCAT.COLUMNS c ON t.TABSCHEMA = c.TABSCHEMA AND t.TABNAME = c.TABNAME
        WHERE t.TYPE = 'T' 
        AND t.TABSCHEMA NOT LIKE 'SYS%'
        AND t.TABSCHEMA NOT IN ('NULLID', 'SQLJ', 'SYSCAT', 'SYSIBM', 'SYSIBMADM', 'SYSSTAT')
        ORDER BY t.TABSCHEMA, t.TABNAME, c.COLNO
        """
        
        self.logger.info("Fetching table definitions from baseline database")
        baseline_tables = self._fetch_all(self.baseline_conn, table_query)
        self.logger.info(f"Found {len(baseline_tables)} columns in baseline database")
        
        self.logger.info("Fetching table definitions from modified database")
        modified_tables = self._fetch_all(self.modified_conn, table_query)
        self.logger.info(f"Found {len(modified_tables)} columns in modified database")
        
        # Group by table name
        baseline_dict = self._group_by_table(baseline_tables)
        self.logger.info(f"Found {len(baseline_dict)} tables in baseline database")
        
        modified_dict = self._group_by_table(modified_tables)
        self.logger.info(f"Found {len(modified_dict)} tables in modified database")
        
        # Log some sample data for verification
        if baseline_dict:
            sample_table = next(iter(baseline_dict))
            self.logger.info(f"Sample baseline table: {sample_table}")
            self.logger.info(f"Sample columns: {baseline_dict[sample_table]}")
        
        comparison_file = self.output_dir / 'tables_diff.log'
        self.logger.info(f"Writing comparison results to {comparison_file}")
        
        with open(comparison_file, 'w', encoding='utf-8') as f:
            # Write summary header
            new_tables = set(modified_dict.keys()) - set(baseline_dict.keys())
            dropped_tables = set(baseline_dict.keys()) - set(modified_dict.keys())
            modified_tables = self._find_modified_tables(baseline_dict, modified_dict)
            
            self.logger.info(f"Found {len(new_tables)} new tables")
            self.logger.info(f"Found {len(dropped_tables)} dropped tables")
            self.logger.info(f"Found {len(modified_tables)} modified tables")
            
            f.write("=== Table Comparison Summary ===\n")
            f.write(f"New tables: {len(new_tables)}\n")
            f.write(f"Dropped tables: {len(dropped_tables)}\n")
            f.write(f"Modified tables: {len(modified_tables)}\n\n")
            
            # Log the actual table names for verification
            if new_tables:
                self.logger.info("New tables: " + ", ".join(sorted(new_tables)))
            if dropped_tables:
                self.logger.info("Dropped tables: " + ", ".join(sorted(dropped_tables)))
            if modified_tables:
                self.logger.info("Modified tables: " + ", ".join(sorted(modified_tables)))
            
            if new_tables:
                f.write("\n=== New Tables ===\n")
                for table in sorted(new_tables):
                    f.write(f"\n{table}:\n")
                    self._write_table_definition(f, modified_dict[table])

            if dropped_tables:
                f.write("\n=== Dropped Tables ===\n")
                for table in sorted(dropped_tables):
                    f.write(f"\n{table}:\n")
                    self._write_table_definition(f, baseline_dict[table])

            if modified_tables:
                f.write("\n=== Modified Tables ===\n")
                for table in sorted(modified_tables):
                    f.write(f"\n{table}:\n")
                    self._write_table_differences_detail(
                        f, baseline_dict[table], modified_dict[table])
        
        self.logger.info("Table comparison completed")

    def _group_by_table(self, tables: List[Dict]) -> Dict:
        """Group table records by schema.table"""
        result = {}
        for table in tables:
            key = f"{table['tabschema']}.{table['tabname']}"
            if key not in result:
                result[key] = []
            result[key].append(table)
        return result

    def _find_modified_tables(self, baseline: Dict, modified: Dict) -> List[str]:
        """Find tables that exist in both databases but have different definitions"""
        modified_tables = []
        common_tables = set(baseline.keys()) & set(modified.keys())
        
        for table in common_tables:
            if self._table_definitions_differ(baseline[table], modified[table]):
                modified_tables.append(table)
        
        return modified_tables

    def _table_definitions_differ(self, baseline_cols: List[Dict], 
                                modified_cols: List[Dict]) -> bool:
        """Compare two table definitions to determine if they differ"""
        if len(baseline_cols) != len(modified_cols):
            return True
            
        for b_col, m_col in zip(baseline_cols, modified_cols):
            compare_keys = ['colname', 'typename', 'length', 'scale', 'nulls', 
                          'default', 'identity', 'generated']
            
            # Log differences for debugging
            differences = []
            for key in compare_keys:
                if b_col[key] != m_col[key]:
                    differences.append(f"{key}: {b_col[key]} -> {m_col[key]}")
            
            if differences:
                self.logger.debug(f"Column {b_col['colname']} differences: {differences}")
                return True
                
        return False

    def _write_table_definition(self, file, columns: List[Dict]):
        """Write a table's column definitions to the log file"""
        for col in columns:
            file.write(f"  {col['colname']} {col['typename']}")
            if col['length'] > 0:
                file.write(f"({col['length']}")
                if col['scale'] is not None and col['scale'] > 0:
                    file.write(f",{col['scale']}")
                file.write(")")
            file.write(f" {col['nulls']}")
            if col['default']:
                file.write(f" DEFAULT {col['default']}")
            if col['identity'] == 'Y':
                file.write(" GENERATED ALWAYS AS IDENTITY")
            elif col['generated'] == 'A':
                file.write(" GENERATED ALWAYS")
            elif col['generated'] == 'D':
                file.write(" GENERATED BY DEFAULT")
            file.write("\n")

    def _write_table_differences_detail(self, file, baseline_cols: List[Dict], 
                                      modified_cols: List[Dict]):
        """Write detailed column differences between two table versions"""
        file.write("  Baseline columns:\n")
        self._write_table_definition(file, baseline_cols)
        file.write("\n  Modified columns:\n")
        self._write_table_definition(file, modified_cols)
        
        # Show specific differences
        file.write("\n  Changes:\n")
        baseline_cols_dict = {col['colname']: col for col in baseline_cols}
        modified_cols_dict = {col['colname']: col for col in modified_cols}
        
        # Find added and removed columns
        added_cols = set(modified_cols_dict.keys()) - set(baseline_cols_dict.keys())
        removed_cols = set(baseline_cols_dict.keys()) - set(modified_cols_dict.keys())
        
        if added_cols:
            file.write("  Added columns:\n")
            for col in added_cols:
                file.write(f"    + {col}\n")
        
        if removed_cols:
            file.write("  Removed columns:\n")
            for col in removed_cols:
                file.write(f"    - {col}\n")
        
        # Find modified columns
        common_cols = set(baseline_cols_dict.keys()) & set(modified_cols_dict.keys())
        modified_cols_list = []
        
        for col in common_cols:
            b_col = baseline_cols_dict[col]
            m_col = modified_cols_dict[col]
            changes = []
            
            for key in ['typename', 'length', 'scale', 'nulls', 'default', 
                       'identity', 'generated']:
                if b_col[key] != m_col[key]:
                    changes.append(f"{key}: {b_col[key]} -> {m_col[key]}")
            
            if changes:
                modified_cols_list.append((col, changes))
        
        if modified_cols_list:
            file.write("  Modified columns:\n")
            for col, changes in modified_cols_list:
                file.write(f"    * {col}:\n")
                for change in changes:
                    file.write(f"      {change}\n")

    def compare_procedures(self):
        """Compare stored procedures between databases"""
        self.logger.info("Starting stored procedure comparison")
        
        proc_query = """
        SELECT 
            p.PROCSCHEMA, 
            p.PROCNAME,
            p.SPECIFICNAME,
            p.LANGUAGE,
            p.DETERMINISTIC,
            p.NULLCALL,
            p.ORIGIN,
            p.CREATE_TIME,
            p.REMARKS,
            r.TEXT as ROUTINE_TEXT,
            r.ROUTINESCHEMA,
            r.ROUTINENAME,
            r.ROUTINETYPE,
            r.RESULT_SETS,
            (SELECT COUNT(*) FROM SYSCAT.ROUTINEPARMS rp 
             WHERE rp.ROUTINESCHEMA = p.PROCSCHEMA 
             AND rp.ROUTINENAME = p.PROCNAME 
             AND rp.SPECIFICNAME = p.SPECIFICNAME) as PARAM_COUNT
        FROM SYSCAT.PROCEDURES p
        LEFT JOIN SYSCAT.ROUTINES r ON 
            p.PROCSCHEMA = r.ROUTINESCHEMA AND 
            p.PROCNAME = r.ROUTINENAME AND
            p.SPECIFICNAME = r.SPECIFICNAME
        WHERE p.PROCSCHEMA NOT LIKE 'SYS%'
        AND p.PROCSCHEMA NOT IN ('NULLID', 'SQLJ', 'SYSCAT', 'SYSIBM', 'SYSIBMADM', 'SYSSTAT')
        ORDER BY p.PROCSCHEMA, p.PROCNAME
        """
        
        self.logger.info("Fetching stored procedures from baseline database")
        baseline_procs = self._fetch_all(self.baseline_conn, proc_query)
        self.logger.info(f"Found {len(baseline_procs)} procedures in baseline database")
        
        self.logger.info("Fetching stored procedures from modified database")
        modified_procs = self._fetch_all(self.modified_conn, proc_query)
        self.logger.info(f"Found {len(modified_procs)} procedures in modified database")
        
        # Group by schema.procedure
        baseline_dict = self._group_by_proc(baseline_procs)
        modified_dict = self._group_by_proc(modified_procs)
        
        # Create separate log files for each category
        summary_file = self.output_dir / 'procedures_summary.log'
        new_procs_file = self.output_dir / 'procedures_new.log'
        dropped_procs_file = self.output_dir / 'procedures_dropped.log'
        modified_procs_file = self.output_dir / 'procedures_modified.log'
        
        # Find differences
        new_procs = set(modified_dict.keys()) - set(baseline_dict.keys())
        dropped_procs = set(baseline_dict.keys()) - set(modified_dict.keys())
        modified_procs = self._find_modified_procs(baseline_dict, modified_dict)
        
        # Write summary
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write("=== Stored Procedure Comparison Summary ===\n")
            f.write(f"New procedures: {len(new_procs)}\n")
            f.write(f"Dropped procedures: {len(dropped_procs)}\n")
            f.write(f"Modified procedures: {len(modified_procs)}\n\n")
            
            if new_procs:
                f.write("\nNew Procedures:\n")
                for proc in sorted(new_procs):
                    f.write(f"  {proc}\n")
            
            if dropped_procs:
                f.write("\nDropped Procedures:\n")
                for proc in sorted(dropped_procs):
                    f.write(f"  {proc}\n")
            
            if modified_procs:
                f.write("\nModified Procedures:\n")
                for proc in sorted(modified_procs):
                    f.write(f"  {proc}\n")
        
        # Write new procedures
        if new_procs:
            self.logger.info(f"Writing {len(new_procs)} new procedures to {new_procs_file}")
            with open(new_procs_file, 'w', encoding='utf-8') as f:
                f.write("=== New Procedures ===\n")
                for proc in sorted(new_procs):
                    f.write(f"\n{proc}:\n")
                    self._write_proc_definition(f, modified_dict[proc])
        
        # Write dropped procedures
        if dropped_procs:
            self.logger.info(f"Writing {len(dropped_procs)} dropped procedures to {dropped_procs_file}")
            with open(dropped_procs_file, 'w', encoding='utf-8') as f:
                f.write("=== Dropped Procedures ===\n")
                for proc in sorted(dropped_procs):
                    f.write(f"\n{proc}:\n")
                    self._write_proc_definition(f, baseline_dict[proc])
        
        # Write modified procedures
        if modified_procs:
            self.logger.info(f"Writing {len(modified_procs)} modified procedures to {modified_procs_file}")
            with open(modified_procs_file, 'w', encoding='utf-8') as f:
                f.write("=== Modified Procedures ===\n")
                for proc in sorted(modified_procs):
                    f.write(f"\n{proc}:\n")
                    self._write_proc_differences(f, baseline_dict[proc], modified_dict[proc])
        
        self.logger.info("Stored procedure comparison completed")

    def _group_by_proc(self, procs: List[Dict]) -> Dict:
        """Group procedures by schema.name"""
        result = {}
        for proc in procs:
            key = f"{proc['procschema']}.{proc['procname']}"
            result[key] = proc
        return result

    def _find_modified_procs(self, baseline: Dict, modified: Dict) -> List[str]:
        """Find procedures that exist in both databases but have different definitions"""
        modified_procs = []
        common_procs = set(baseline.keys()) & set(modified.keys())
        
        for proc in common_procs:
            if self._proc_definitions_differ(baseline[proc], modified[proc]):
                modified_procs.append(proc)
        
        return modified_procs

    def _proc_definitions_differ(self, baseline_proc: Dict, modified_proc: Dict) -> bool:
        """Compare two procedure definitions to determine if they differ"""
        compare_keys = [
            'language', 'deterministic', 'nullcall', 'routine_text', 
            'param_count', 'result_sets'
        ]
        
        differences = []
        for key in compare_keys:
            if baseline_proc[key] != modified_proc[key]:
                differences.append(f"{key}: Changed")
                self.logger.debug(f"Procedure difference in {key}:")
                self.logger.debug(f"Baseline: {baseline_proc[key]}")
                self.logger.debug(f"Modified: {modified_proc[key]}")
        
        return bool(differences)

    def _write_proc_definition(self, file, proc: Dict):
        """Write a procedure's definition to the log file"""
        file.write(f"  Language: {proc['language']}\n")
        file.write(f"  Deterministic: {proc['deterministic']}\n")
        file.write(f"  Null Call: {proc['nullcall']}\n")
        file.write(f"  Parameter Count: {proc['param_count']}\n")
        file.write(f"  Result Sets: {proc['result_sets']}\n")
        if proc['remarks']:
            file.write(f"  Remarks: {proc['remarks']}\n")
        file.write("\n  Definition:\n")
        if proc['routine_text']:
            # Handle potential None values and encode special characters
            text = proc['routine_text'] or ''
            file.write(f"{text}\n")

    def _write_proc_differences(self, file, baseline_proc: Dict, modified_proc: Dict):
        """Write detailed differences between two procedure versions"""
        file.write("  Baseline:\n")
        self._write_proc_definition(file, baseline_proc)
        file.write("\n  Modified:\n")
        self._write_proc_definition(file, modified_proc)
        
        # Show specific differences
        file.write("\n  Changes:\n")
        compare_keys = [
            'language', 'deterministic', 'nullcall', 'routine_text', 
            'param_count', 'result_sets'
        ]
        
        for key in compare_keys:
            if baseline_proc[key] != modified_proc[key]:
                file.write(f"    * {key} changed\n")
                if key in ['routine_text']:
                    file.write("      (See full definitions above)\n")
                else:
                    file.write(f"      From: {baseline_proc[key]}\n")
                    file.write(f"      To: {modified_proc[key]}\n")

    def compare_triggers(self):
        """Compare triggers between databases"""
        self.logger.info("Starting trigger comparison")
        
        trigger_query = """
        SELECT 
            t.TRIGSCHEMA,
            t.TRIGNAME,
            t.TABSCHEMA,
            t.TABNAME,
            t.TRIGTIME,
            t.TRIGEVENT,
            t.GRANULARITY,
            t.VALID,
            t.CREATE_TIME,
            t.REMARKS,
            t.TEXT as TRIGGER_TEXT,
            t.ENABLED,
            t.QUALIFIER,
            t.FUNC_PATH
        FROM SYSCAT.TRIGGERS t
        WHERE t.TRIGSCHEMA NOT LIKE 'SYS%'
        AND t.TRIGSCHEMA NOT IN ('NULLID', 'SQLJ', 'SYSCAT', 'SYSIBM', 'SYSIBMADM', 'SYSSTAT')
        ORDER BY t.TRIGSCHEMA, t.TRIGNAME
        """
        
        self.logger.info("Fetching triggers from baseline database")
        baseline_triggers = self._fetch_all(self.baseline_conn, trigger_query)
        self.logger.info(f"Found {len(baseline_triggers)} triggers in baseline database")
        
        self.logger.info("Fetching triggers from modified database")
        modified_triggers = self._fetch_all(self.modified_conn, trigger_query)
        self.logger.info(f"Found {len(modified_triggers)} triggers in modified database")
        
        # Group by schema.trigger
        baseline_dict = self._group_by_trigger(baseline_triggers)
        modified_dict = self._group_by_trigger(modified_triggers)
        
        # Create separate log files for each category
        summary_file = self.output_dir / 'triggers_summary.log'
        new_triggers_file = self.output_dir / 'triggers_new.log'
        dropped_triggers_file = self.output_dir / 'triggers_dropped.log'
        modified_triggers_file = self.output_dir / 'triggers_modified.log'
        
        # Find differences
        new_triggers = set(modified_dict.keys()) - set(baseline_dict.keys())
        dropped_triggers = set(baseline_dict.keys()) - set(modified_dict.keys())
        modified_triggers = self._find_modified_triggers(baseline_dict, modified_dict)
        
        # Write summary
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write("=== Trigger Comparison Summary ===\n")
            f.write(f"New triggers: {len(new_triggers)}\n")
            f.write(f"Dropped triggers: {len(dropped_triggers)}\n")
            f.write(f"Modified triggers: {len(modified_triggers)}\n\n")
            
            if new_triggers:
                f.write("\nNew Triggers:\n")
                for trigger in sorted(new_triggers):
                    f.write(f"  {trigger}\n")
            
            if dropped_triggers:
                f.write("\nDropped Triggers:\n")
                for trigger in sorted(dropped_triggers):
                    f.write(f"  {trigger}\n")
            
            if modified_triggers:
                f.write("\nModified Triggers:\n")
                for trigger in sorted(modified_triggers):
                    f.write(f"  {trigger}\n")
        
        # Write new triggers
        if new_triggers:
            self.logger.info(f"Writing {len(new_triggers)} new triggers to {new_triggers_file}")
            with open(new_triggers_file, 'w', encoding='utf-8') as f:
                f.write("=== New Triggers ===\n")
                for trigger in sorted(new_triggers):
                    f.write(f"\n{trigger}:\n")
                    self._write_trigger_definition(f, modified_dict[trigger])
        
        # Write dropped triggers
        if dropped_triggers:
            self.logger.info(f"Writing {len(dropped_triggers)} dropped triggers to {dropped_triggers_file}")
            with open(dropped_triggers_file, 'w', encoding='utf-8') as f:
                f.write("=== Dropped Triggers ===\n")
                for trigger in sorted(dropped_triggers):
                    f.write(f"\n{trigger}:\n")
                    self._write_trigger_definition(f, baseline_dict[trigger])
        
        # Write modified triggers
        if modified_triggers:
            self.logger.info(f"Writing {len(modified_triggers)} modified triggers to {modified_triggers_file}")
            with open(modified_triggers_file, 'w', encoding='utf-8') as f:
                f.write("=== Modified Triggers ===\n")
                for trigger in sorted(modified_triggers):
                    f.write(f"\n{trigger}:\n")
                    self._write_trigger_differences(f, baseline_dict[trigger], modified_dict[trigger])
        
        self.logger.info("Trigger comparison completed")

    def _group_by_trigger(self, triggers: List[Dict]) -> Dict:
        """Group triggers by schema.name"""
        result = {}
        for trigger in triggers:
            key = f"{trigger['trigschema']}.{trigger['trigname']}"
            result[key] = trigger
        return result

    def _find_modified_triggers(self, baseline: Dict, modified: Dict) -> List[str]:
        """Find triggers that exist in both databases but have different definitions"""
        modified_triggers = []
        common_triggers = set(baseline.keys()) & set(modified.keys())
        
        for trigger in common_triggers:
            if self._trigger_definitions_differ(baseline[trigger], modified[trigger]):
                modified_triggers.append(trigger)
        
        return modified_triggers

    def _trigger_definitions_differ(self, baseline_trigger: Dict, modified_trigger: Dict) -> bool:
        """Compare two trigger definitions to determine if they differ"""
        compare_keys = [
            'trigtime', 'trigevent', 'granularity', 'valid', 'enabled',
            'trigger_text', 'tabschema', 'tabname'
        ]
        
        differences = []
        for key in compare_keys:
            if baseline_trigger[key] != modified_trigger[key]:
                differences.append(f"{key}: Changed")
                self.logger.debug(f"Trigger difference in {key}:")
                self.logger.debug(f"Baseline: {baseline_trigger[key]}")
                self.logger.debug(f"Modified: {modified_trigger[key]}")
        
        return bool(differences)

    def _write_trigger_definition(self, file, trigger: Dict):
        """Write a trigger's definition to the log file"""
        file.write(f"  Table: {trigger['tabschema']}.{trigger['tabname']}\n")
        file.write(f"  Trigger Time: {trigger['trigtime']}\n")
        file.write(f"  Trigger Event: {trigger['trigevent']}\n")
        file.write(f"  Granularity: {trigger['granularity']}\n")
        file.write(f"  Valid: {trigger['valid']}\n")
        file.write(f"  Enabled: {trigger['enabled']}\n")
        if trigger['remarks']:
            file.write(f"  Remarks: {trigger['remarks']}\n")
        file.write("\n  Definition:\n")
        if trigger['trigger_text']:
            # Handle potential None values and encode special characters
            text = trigger['trigger_text'] or ''
            file.write(f"{text}\n")

    def _write_trigger_differences(self, file, baseline_trigger: Dict, modified_trigger: Dict):
        """Write detailed differences between two trigger versions"""
        file.write("  Baseline:\n")
        self._write_trigger_definition(file, baseline_trigger)
        file.write("\n  Modified:\n")
        self._write_trigger_definition(file, modified_trigger)
        
        # Show specific differences
        file.write("\n  Changes:\n")
        compare_keys = [
            'trigtime', 'trigevent', 'granularity', 'valid', 'enabled',
            'trigger_text', 'tabschema', 'tabname'
        ]
        
        for key in compare_keys:
            if baseline_trigger[key] != modified_trigger[key]:
                file.write(f"    * {key} changed\n")
                if key in ['trigger_text']:
                    file.write("      (See full definitions above)\n")
                else:
                    file.write(f"      From: {baseline_trigger[key]}\n")
                    file.write(f"      To: {modified_trigger[key]}\n")

    def compare_functions(self):
        """Compare functions between databases"""
        self.logger.info("Starting function comparison")
        
        function_query = """
        SELECT 
            f.FUNCSCHEMA,
            f.FUNCNAME,
            f.SPECIFICNAME,
            f.ORIGIN,
            f.TYPE as FUNCTION_TYPE,
            f.RETURN_TYPE,
            r.TEXT as FUNCTION_TEXT,
            r.DETERMINISTIC,
            r.NULLCALL,
            r.LANGUAGE,
            f.CREATE_TIME,
            f.REMARKS,
            (SELECT COUNT(*) FROM SYSCAT.FUNCPARMS fp 
             WHERE fp.FUNCSCHEMA = f.FUNCSCHEMA 
             AND fp.FUNCNAME = f.FUNCNAME 
             AND fp.SPECIFICNAME = f.SPECIFICNAME) as PARAM_COUNT
        FROM SYSCAT.FUNCTIONS f
        LEFT JOIN SYSCAT.ROUTINES r ON 
            f.FUNCSCHEMA = r.ROUTINESCHEMA AND 
            f.FUNCNAME = r.ROUTINENAME AND
            f.SPECIFICNAME = r.SPECIFICNAME
        WHERE f.FUNCSCHEMA NOT LIKE 'SYS%'
        AND f.FUNCSCHEMA NOT IN ('NULLID', 'SQLJ', 'SYSCAT', 'SYSIBM', 'SYSIBMADM', 'SYSSTAT')
        ORDER BY f.FUNCSCHEMA, f.FUNCNAME
        """
        
        self.logger.info("Fetching functions from baseline database")
        baseline_functions = self._fetch_all(self.baseline_conn, function_query)
        self.logger.info(f"Found {len(baseline_functions)} functions in baseline database")
        
        self.logger.info("Fetching functions from modified database")
        modified_functions = self._fetch_all(self.modified_conn, function_query)
        self.logger.info(f"Found {len(modified_functions)} functions in modified database")
        
        # Group by schema.function
        baseline_dict = self._group_by_function(baseline_functions)
        modified_dict = self._group_by_function(modified_functions)
        
        # Create separate log files for each category
        summary_file = self.output_dir / 'functions_summary.log'
        new_funcs_file = self.output_dir / 'functions_new.log'
        dropped_funcs_file = self.output_dir / 'functions_dropped.log'
        modified_funcs_file = self.output_dir / 'functions_modified.log'
        
        # Find differences
        new_funcs = set(modified_dict.keys()) - set(baseline_dict.keys())
        dropped_funcs = set(baseline_dict.keys()) - set(modified_dict.keys())
        modified_funcs = self._find_modified_functions(baseline_dict, modified_dict)
        
        # Write summary
        with open(summary_file, 'w', encoding='utf-8') as f:
            f.write("=== Function Comparison Summary ===\n")
            f.write(f"New functions: {len(new_funcs)}\n")
            f.write(f"Dropped functions: {len(dropped_funcs)}\n")
            f.write(f"Modified functions: {len(modified_funcs)}\n\n")
            
            if new_funcs:
                f.write("\nNew Functions:\n")
                for func in sorted(new_funcs):
                    f.write(f"  {func}\n")
            
            if dropped_funcs:
                f.write("\nDropped Functions:\n")
                for func in sorted(dropped_funcs):
                    f.write(f"  {func}\n")
            
            if modified_funcs:
                f.write("\nModified Functions:\n")
                for func in sorted(modified_funcs):
                    f.write(f"  {func}\n")
        
        # Write new functions
        if new_funcs:
            self.logger.info(f"Writing {len(new_funcs)} new functions to {new_funcs_file}")
            with open(new_funcs_file, 'w', encoding='utf-8') as f:
                f.write("=== New Functions ===\n")
                for func in sorted(new_funcs):
                    f.write(f"\n{func}:\n")
                    self._write_function_definition(f, modified_dict[func])
        
        # Write dropped functions
        if dropped_funcs:
            self.logger.info(f"Writing {len(dropped_funcs)} dropped functions to {dropped_funcs_file}")
            with open(dropped_funcs_file, 'w', encoding='utf-8') as f:
                f.write("=== Dropped Functions ===\n")
                for func in sorted(dropped_funcs):
                    f.write(f"\n{func}:\n")
                    self._write_function_definition(f, baseline_dict[func])
        
        # Write modified functions
        if modified_funcs:
            self.logger.info(f"Writing {len(modified_funcs)} modified functions to {modified_funcs_file}")
            with open(modified_funcs_file, 'w', encoding='utf-8') as f:
                f.write("=== Modified Functions ===\n")
                for func in sorted(modified_funcs):
                    f.write(f"\n{func}:\n")
                    self._write_function_differences(f, baseline_dict[func], modified_dict[func])
        
        self.logger.info("Function comparison completed")

    def _group_by_function(self, functions: List[Dict]) -> Dict:
        """Group functions by schema.name"""
        result = {}
        for func in functions:
            key = f"{func['funcschema']}.{func['funcname']}"
            result[key] = func
        return result

    def _find_modified_functions(self, baseline: Dict, modified: Dict) -> List[str]:
        """Find functions that exist in both databases but have different definitions"""
        modified_funcs = []
        common_funcs = set(baseline.keys()) & set(modified.keys())
        
        for func in common_funcs:
            if self._function_definitions_differ(baseline[func], modified[func]):
                modified_funcs.append(func)
        
        return modified_funcs

    def _function_definitions_differ(self, baseline_func: Dict, modified_func: Dict) -> bool:
        """Compare two function definitions to determine if they differ"""
        compare_keys = [
            'language', 'deterministic', 'nullcall', 'function_text',
            'return_type', 'param_count', 'function_type'
        ]
        
        differences = []
        for key in compare_keys:
            if baseline_func[key] != modified_func[key]:
                differences.append(f"{key}: Changed")
                self.logger.debug(f"Function difference in {key}:")
                self.logger.debug(f"Baseline: {baseline_func[key]}")
                self.logger.debug(f"Modified: {modified_func[key]}")
        
        return bool(differences)

    def _write_function_definition(self, file, func: Dict):
        """Write a function's definition to the log file"""
        file.write(f"  Function Type: {func['function_type']}\n")
        file.write(f"  Return Type: {func['return_type']}\n")
        file.write(f"  Language: {func['language']}\n")
        file.write(f"  Deterministic: {func['deterministic']}\n")
        file.write(f"  Null Call: {func['nullcall']}\n")
        file.write(f"  Parameter Count: {func['param_count']}\n")
        if func['remarks']:
            file.write(f"  Remarks: {func['remarks']}\n")
        file.write("\n  Definition:\n")
        if func['function_text']:
            # Handle potential None values and encode special characters
            text = func['function_text'] or ''
            file.write(f"{text}\n")

    def _write_function_differences(self, file, baseline_func: Dict, modified_func: Dict):
        """Write detailed differences between two function versions"""
        file.write("  Baseline:\n")
        self._write_function_definition(file, baseline_func)
        file.write("\n  Modified:\n")
        self._write_function_definition(file, modified_func)
        
        # Show specific differences
        file.write("\n  Changes:\n")
        compare_keys = [
            'language', 'deterministic', 'nullcall', 'function_text',
            'return_type', 'param_count', 'function_type'
        ]
        
        for key in compare_keys:
            if baseline_func[key] != modified_func[key]:
                file.write(f"    * {key} changed\n")
                if key in ['function_text']:
                    file.write("      (See full definitions above)\n")
                else:
                    file.write(f"      From: {baseline_func[key]}\n")
                    file.write(f"      To: {modified_func[key]}\n")

def main():
    parser = argparse.ArgumentParser(description='Compare two DB2 database structures')
    parser.add_argument('--config', required=True, help='Path to configuration file')
    parser.add_argument('--output-dir', default='comparison_results',
                       help='Directory for output files')
    args = parser.parse_args()

    # Load configuration
    try:
        with open(args.config) as f:
            config = json.load(f)
    except Exception as e:
        print(f"Error loading configuration file: {str(e)}")
        sys.exit(1)

    baseline_config = DbConfig(**config['baseline'])
    modified_config = DbConfig(**config['modified'])

    comparator = Db2Comparator(baseline_config, modified_config, args.output_dir)
    
    try:
        comparator.connect()
        comparator.compare_tables()
        comparator.compare_procedures()
        comparator.compare_triggers()
        comparator.compare_functions()
        # Add other comparison methods here
    finally:
        comparator.close()

if __name__ == '__main__':
    main() 