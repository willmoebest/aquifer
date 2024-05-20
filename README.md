
# Aquifer![Aquifer-GitHub](https://github.com/willmoebest/aquifer/assets/113856531/19557f45-b7ab-4b8a-8c27-4835fb776da3)


## Overview

Aquifer is a powerful and flexible database synchronization tool designed to synchronize database objects (tables, views, procedures) between a source database and multiple target databases. It supports a wide range of databases including MySQL, PostgreSQL, MongoDB, Neo4j, SQL Server, and Oracle. Aquifer also includes features for testing SQL statements for validity before execution and rolling back changes if necessary.
## In no way connected to Liquibase other than extreme affection!
## Features

- Synchronize tables, views, and procedures between source and target databases.
- Support for MySQL, PostgreSQL, MongoDB, Neo4j, SQL Server, and Oracle.
- Test SQL statements for validity before execution.
- Log changes and provide rollback functionality.
- Command-line interface for easy usage.

## Requirements

- Python 3.6+
- Python packages: `mysql-connector-python`, `psycopg2`, `pymongo`, `pyodbc`, `cx_Oracle`, `neo4j`

## Installation

1. Clone the repository:

    ```sh
    git clone https://github.com/willmoebest/aquifer.git
    ```

2. Install required Python packages:

    ```sh
    pip install mysql-connector-python psycopg2 pymongo pyodbc cx_Oracle neo4j
    ```

## Usage

### Command-Line Arguments

- `--source-config`: Path to JSON file containing source database configuration.
- `--source-db-type`: Type of the source database (e.g., `mysql`, `postgresql`, `mongodb`, `neo4j`, `sqlserver`, `oracle`).
- `--target-configs`: Path to JSON file containing target database configurations.
- `--sync-all-tables`: Sync all tables from the source database to the target databases.
- `--sync-all-views`: Sync all views from the source database to the target databases.
- `--sync-all-procedures`: Sync all procedures from the source database to the target databases.
- `--alter-sync`: Use `ALTER` statements to sync tables, views, and procedures.
- `--create-on-target`: Create objects on target databases if they don't exist in the source database.
- `--rollback`: Rollback changes for a specific object (format: `type:name`).

### Example Configurations

#### Source Configuration (`source_config.json`)

```json
{
    "host": "source_host",
    "user": "source_user",
    "password": "source_password",
    "database": "source_database"
}
```

#### Target Configurations (`target_configs.json`)

```json
[
    {
        "type": "mysql",
        "config": {
            "host": "target_host_1",
            "user": "target_user_1",
            "password": "target_password_1",
            "database": "target_database_1"
        }
    },
    {
        "type": "postgresql",
        "config": {
            "host": "target_host_2",
            "user": "target_user_2",
            "password": "target_password_2",
            "dbname": "target_database_2"
        }
    },
    {
        "type": "sqlserver",
        "config": {
            "host": "target_host_3",
            "user": "target_user_3",
            "password": "target_password_3",
            "database": "target_database_3"
        }
    },
    {
        "type": "oracle",
        "config": {
            "host": "target_host_4",
            "port": 1521,
            "sid": "target_sid",
            "user": "target_user_4",
            "password": "target_password_4"
        }
    }
]
```

### Running Aquifer

#### Synchronize All Tables

```sh
python aquifer.py --source-config source_config.json --source-db-type mysql --target-configs target_configs.json --sync-all-tables
```

#### Synchronize All Views

```sh
python aquifer.py --source-config source_config.json --source-db-type mysql --target-configs target_configs.json --sync-all-views
```

#### Synchronize All Procedures

```sh
python aquifer.py --source-config source_config.json --source-db-type mysql --target-configs target_configs.json --sync-all-procedures
```

#### Rollback Changes for a Specific Table

```sh
python aquifer.py --source-config source_config.json --source-db-type mysql --target-configs target_configs.json --rollback table:my_table
```

## Code Structure

- `DatabaseSync` (Abstract Base Class): Defines the interface for database synchronization.
- `MySQLSync`, `PostgreSQLSync`, `MongoDBSync`, `Neo4jSync`, `SQLServerSync`, `OracleSync` (Concrete Implementations): Implement the interface for each supported database.
- `DatabaseSyncFactory`: Factory class to create instances of the appropriate synchronization class based on configuration.
- `main`: Entry point for the command-line interface.

## License

This project is licensed under the GNU License. See the LICENSE file for details.

---

## Detailed Instructions

1. **Setup Source and Target Configurations**:
    - Create JSON configuration files for the source and target databases. Refer to the example configurations above.

2. **Running Synchronization**:
    - Use the appropriate command-line arguments to synchronize tables, views, and procedures from the source database to the target databases.
    - Example: To synchronize all tables, run:
        ```sh
        python aquifer.py --source-config source_config.json --source-db-type mysql --target-configs target_configs.json --sync-all-tables
        ```

3. **Testing SQL Statements**:
    - Aquifer tests SQL statements for validity before execution. If a statement is invalid, an error will be logged and the statement will not be executed.

4. **Rollback Changes**:
    - To rollback changes for a specific object, use the `--rollback` argument with the format `type:name`.
    - Example: To rollback changes for a table named `my_table`, run:
        ```sh
        python aquifer.py --source-config source_config.json --source-db-type mysql --target-configs target_configs.json --rollback table:my_table
        ```

5. **Logging**:
    - Aquifer logs all synchronization actions, including the original state, new state, and rollback actions, to the `sync_log` table in the target databases.

## Notes

- Ensure that the `sync_log` table is created in the target databases. Aquifer will create this table if it does not exist.
- Aquifer currently supports basic synchronization operations. Depending on your requirements, you may need to extend the functionality for more complex scenarios.

With these instructions and the provided script, you should be able to set up and use Aquifer effectively for your database synchronization and rollback needs.
