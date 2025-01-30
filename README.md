# DB2 Database Structure Comparator

A Python script to compare the structure of two IBM DB2 databases and identify differences in tables, stored procedures, triggers, and functions.

## Overview

This tool helps database administrators and developers identify structural differences between two DB2 databases by comparing:
- Tables and their column definitions
- Stored Procedures
- Triggers
- Functions
- Views

For each category, the tool generates separate log files showing:
- New objects (present in modified DB but not in baseline)
- Dropped objects (present in baseline DB but not in modified)
- Modified objects (present in both but with different definitions)

## Prerequisites

- Python 3.13.1 or higher
- IBM DB2 11.5.9 or higher
- IBM DB2 Python driver (ibm_db)

## Installation

1. Install the IBM DB2 Python driver:
```bash
pip install ibm-db
```

2. Ensure your DB2 client is properly configured and the following environment variables are set:
   - DB2_HOME
   - PATH (including DB2 bin directory)

## Configuration

Create a `config.json` file with your database connection details:

```json
{
    "baseline": {
        "host": "localhost",
        "port": 50000,
        "database": "baseline_db",
        "username": "db2admin",
        "password": "your_password"
    },
    "modified": {
        "host": "localhost",
        "port": 50000,
        "database": "modified_db",
        "username": "db2admin",
        "password": "your_password"
    }
}
```

## Usage

Run the script with:

```bash
python db2_compare.py --config config.json --output-dir comparison_results
```

### Parameters:
- `--config`: Path to your configuration file (required)
- `--output-dir`: Directory for output files (default: 'comparison_results')

## Output Files

The script generates several log files in the output directory:

### Tables
- `tables_summary.log`: Overview of all table changes
- `tables_new.log`: Details of new tables
- `tables_dropped.log`: Details of dropped tables
- `tables_modified.log`: Details of modified tables

### Stored Procedures
- `procedures_summary.log`: Overview of all procedure changes
- `procedures_new.log`: Details of new procedures
- `procedures_dropped.log`: Details of dropped procedures
- `procedures_modified.log`: Details of modified procedures

### Triggers
- `triggers_summary.log`: Overview of all trigger changes
- `triggers_new.log`: Details of new triggers
- `triggers_dropped.log`: Details of dropped triggers
- `triggers_modified.log`: Details of modified triggers

### Functions
- `functions_summary.log`: Overview of all function changes
- `functions_new.log`: Details of new functions
- `functions_dropped.log`: Details of dropped functions
- `functions_modified.log`: Details of modified functions

### Views
- `views_summary.log`: Overview of all view changes
- `views_new.log`: Details of new views
- `views_dropped.log`: Details of dropped views
- `views_modified.log`: Details of modified views

## Comparison Details

### Table Comparisons Include:
- Column names and order
- Data types and lengths
- Null constraints
- Default values
- Identity columns
- Generated columns

### Stored Procedure Comparisons Include:
- Language
- Parameter count
- Deterministic property
- Null call behavior
- Full procedure text

### Trigger Comparisons Include:
- Associated table
- Trigger timing
- Trigger events
- Granularity
- Validity status
- Enabled status
- Full trigger definition

### Function Comparisons Include:
- Return type
- Parameter count
- Language
- Deterministic property
- Null call behavior
- Full function definition

### View Comparisons Include:
- Full view definition (SELECT statement)
- Validity status
- Check option settings
- Read-only status
- View remarks

## Limitations

- The tool only compares structural differences, not data content
- System schemas are excluded from comparison
- Some DB2 object types (like sequences) are not currently compared

## Error Handling

The script includes error handling for:
- Database connection issues
- Configuration file problems
- Missing or invalid permissions
- Character encoding issues

## Contributing

Feel free to submit issues and enhancement requests!

## License

[Your chosen license]
