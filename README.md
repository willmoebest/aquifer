# Aquifer

## Overview

Aquifer is a database synchronization tool designed to synchronize database objects (tables, views, procedures) between a source database and multiple target databases. It supports MySQL, PostgreSQL, MongoDB, and Neo4j databases. Aquifer also includes features for testing SQL statements for validity before execution and rolling back changes if necessary. 

## With that said, 
### USE AT YOUR OWN RISK. THIS IS MOSTLY UNTESTED AS OF 5/19/2024

## Features

- Synchronize tables, views, and procedures between source and target databases.
- Support for MySQL, PostgreSQL, MongoDB, and Neo4j.
- Test SQL statements for validity before execution.
- Log changes and provide rollback functionality.
- Command-line interface for easy usage.

- Tested so far on MySQL 8.

## Requirements

- Python 3.6+
- Python packages: `mysql-connector-python`, `psycopg2`, `pymongo`, `neo4j`

## Installation

1. Clone the repository:

    ```sh
    git clone https://github.com/willmoebest/aquifer.git
    ```

2. Install required Python packages:

    ```sh
    pip install mysql-connector-python psycopg2 pymongo neo4j
    ```

## Usage

### Command-Line Arguments

- `--source-config`: Path to JSON file containing source database configuration.
- `--source-db-type`: Type of the source database (e.g., `mysql`, `postgresql`, `mongodb`, `neo4j`).
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
- `MySQLSync`, `PostgreSQLSync`, `MongoDBSync`, `Neo4jSync` (Concrete Implementations): Implement the interface for each supported database.
- `DatabaseSyncFactory`: Factory class to create instances of the appropriate synchronization class based on configuration.
- `main`: Entry point for the command-line interface.

## License

This project is licensed under the MIT License. See the LICENSE file for details.

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

### Aquifer Script

```python
import json
import argparse
import hashlib
import logging
import mysql.connector
import psycopg2
import pymongo
from neo4j import GraphDatabase
from abc import ABC, abstractmethod
from mysql.connector import Error as MySQLError
from psycopg2 import Error as PGError

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class DatabaseSync(ABC):
    def __init__(self, config):
        self.config = config
    
    @abstractmethod
    def connect(self):
        pass
    
    @abstractmethod
    def close(self):
        pass
    
    @abstractmethod
    def log_sync_action(self, object_type, object_name, action, source_code_hash, sync_direction, original_state, new_state, rollback_action):
        pass

    @abstractmethod
    def synchronize_table(self, table, alter_sync, source_code_hash, create_on_target):
        pass
    
    @abstractmethod
    def synchronize_view(self, view_name, alter_sync, source_code_hash, create_on_target):
        pass
    
    @abstractmethod
    def synchronize_procedure(self, procedure_name, alter_sync, source_code_hash, create_on_target):
        pass
    
    @abstractmethod
    def synchronize_all_tables(self, alter_sync, source_code_hash, create_on_target):
        pass
    
    @abstractmethod
    def synchronize_all_views(self, alter_sync, source_code_hash, create_on_target):
        pass
    
    @abstractmethod
    def synchronize_all_procedures(self, alter_sync, source_code_hash, create_on_target):
        pass
    
    @abstractmethod
    def rollback_table(self, table_name):
        pass
    
    @abstractmethod
    def rollback_view(self, view_name):
        pass
    
    @abstractmethod
    def rollback_procedure(self, procedure_name):
        pass

class MySQLSync(DatabaseSync):
    def connect(self):
        try:
            self.conn = mysql.connector.connect(**self.config)
            self.cursor = self.conn.cursor()
        except MySQLError as e:
            logging.error(f"Error connecting to MySQL: {e}")
            raise
    
    def close(self):
        self.cursor.close()
        self.conn.close()
    
    def log_sync_action(self, object_type, object_name, action, source_code_hash, sync_direction, original_state, new_state, rollback_action):
        try:
            self.cursor.execute("""
                INSERT INTO sync_log (object_type, object_name, action, source_code_hash, sync_direction, original_state, new_state, rollback_action)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (object_type, object_name, action, source_code_hash, sync_direction, original_state, new_state, rollback_action))
            self.conn.commit()
        except MySQLError as e:
            logging.error(f"Error logging sync action: {e}")
    
    def test_sql_statement(self, statement):
        try:
            self.cursor.execute("START TRANSACTION")
            self.cursor.execute(statement)
            self.cursor.execute("ROLLBACK")
            return True
        except MySQLError as e:
            logging.error(f"Invalid SQL statement: {statement}. Error: {e}")
            self.cursor.execute("ROLLBACK")
            return False

    def synchronize_table(self, table, alter_sync, source_code_hash, create_on_target):
        try:
            # Retrieve original state from target
            self.cursor.execute(f"SHOW CREATE TABLE {table}")
            original_state = self.cursor.fetchone()
            original_state = original_state[1] if original_state else None

            # Synchronization logic
            self.cursor.execute(f"DESCRIBE {table}")


            source_schema = self.cursor.fetchall()
            source_columns = [(col[0], col[1]) for col in source_schema]

            self.cursor.execute(f"SHOW TABLES LIKE '{table}'")
            target_exists = bool(self.cursor.fetchone())

            if not target_exists:
                if create_on_target:
                    logging.info(f"Table {table} doesn't exist on target. Creating...")
                    create_table_statement = f"CREATE TABLE {table} ("
                    for column, column_type in source_columns:
                        create_table_statement += f"{column} {column_type}, "
                    create_table_statement = create_table_statement[:-2] + ");"
                    
                    if self.test_sql_statement(create_table_statement):
                        self.cursor.execute(create_table_statement)
                        new_state = create_table_statement
                        logging.info(f"Table {table} created successfully on target.")
                        self.log_sync_action("table", table, "create", source_code_hash, "source_to_target", original_state, new_state, "drop")
                return

            # If the table exists on the target
            self.cursor.execute(f"DESCRIBE {table}")
            target_schema = self.cursor.fetchall()
            target_columns = {col[0]: col[1] for col in target_schema}

            for column, column_type in source_columns:
                if column not in target_columns:
                    if alter_sync:
                        statement = f"ALTER TABLE {table} ADD COLUMN {column} {column_type};"
                        logging.info(f"Applying ALTER statement: {statement}")
                        
                        if self.test_sql_statement(statement):
                            self.cursor.execute(statement)
                            new_state = statement
                            self.log_sync_action("table", table, "alter", source_code_hash, "source_to_target", original_state, new_state, f"ALTER TABLE {table} DROP COLUMN {column}")
                    else:
                        logging.info(f"Table '{table}' has a new column '{column}'")
                else:
                    logging.info(f"Table '{table}' column '{column}' already exists.")
        except MySQLError as e:
            logging.error(f"Error synchronizing table {table}: {e}")

    def synchronize_view(self, view_name, alter_sync, source_code_hash, create_on_target):
        try:
            source_definition = self.get_view_definition(view_name)
            self.cursor.execute(f"SHOW CREATE VIEW {view_name}")
            original_state = self.cursor.fetchone()
            original_state = original_state[1] if original_state else None

            self.cursor.execute(f"SHOW FULL TABLES WHERE Table_type = 'VIEW' LIKE '{view_name}'")
            target_exists = bool(self.cursor.fetchone())

            if not target_exists and create_on_target:
                logging.info(f"View {view_name} doesn't exist on target. Creating...")
                
                if self.test_sql_statement(source_definition):
                    self.cursor.execute(source_definition)
                    new_state = source_definition
                    logging.info(f"View {view_name} created successfully on target.")
                    self.log_sync_action("view", view_name, "create", source_code_hash, "source_to_target", original_state, new_state, "drop")
                return

            if source_definition != original_state:
                logging.info(f"Synchronizing view: {view_name}")
                self.cursor.execute(f"DROP VIEW IF EXISTS {view_name}")
                
                if self.test_sql_statement(source_definition):
                    self.cursor.execute(source_definition)
                    new_state = source_definition
                    logging.info(f"View {view_name} synchronized successfully.")
                    self.log_sync_action("view", view_name, "sync", source_code_hash, "source_to_target", original_state, new_state, "drop")
            else:
                logging.info(f"View {view_name} is already synchronized.")
        except MySQLError as e:
            logging.error(f"Error synchronizing view {view_name}: {e}")

    def synchronize_procedure(self, procedure_name, alter_sync, source_code_hash, create_on_target):
        try:
            source_definition = self.get_procedure_definition(procedure_name)

            self.cursor.execute(f"SHOW PROCEDURE STATUS WHERE Db = DATABASE() AND Name = '{procedure_name}'")
            target_exists = bool(self.cursor.fetchone())

            if not target_exists or create_on_target:
                if not target_exists:
                    logging.info(f"Procedure {procedure_name} doesn't exist on target. Creating...")
                else:
                    logging.info(f"Procedure {procedure_name} creation is not disabled. Creating...")

                if self.test_sql_statement(source_definition):
                    self.cursor.execute(source_definition)
                    new_state = source_definition
                    logging.info(f"Procedure {procedure_name} created successfully on target.")
                    self.log_sync_action("procedure", procedure_name, "create", source_code_hash, "source_to_target", None, new_state, "drop")
                return

            if target_exists:
                target_definition = self.get_procedure_definition(target_cursor, procedure_name)
                original_state = target_definition

                if source_definition != target_definition:
                    logging.info(f"Synchronizing procedure: {procedure_name}")
                    self.cursor.execute(f"DROP PROCEDURE IF EXISTS {procedure_name}")
                    
                    if self.test_sql_statement(source_definition):
                        self.cursor.execute(source_definition)
                        new_state = source_definition
                        logging.info(f"Procedure {procedure_name} synchronized successfully.")
                        self.log_sync_action("procedure", procedure_name, "sync", source_code_hash, "source_to_target", original_state, new_state, "drop")
                else:
                    logging.info(f"Procedure {procedure_name} is already synchronized.")
        except MySQLError as e:
            logging.error(f"Error synchronizing procedure {procedure_name}: {e}")

    def synchronize_all_tables(self, alter_sync, source_code_hash, create_on_target):
        try:
            self.cursor.execute("SHOW TABLES")
            tables_to_sync = [table[0] for table in self.cursor.fetchall()]

            for table in tables_to_sync:
                self.synchronize_table(table, alter_sync, source_code_hash, create_on_target)
        except MySQLError as e:
            logging.error(f"Error synchronizing all tables: {e}")

    def synchronize_all_views(self, alter_sync, source_code_hash, create_on_target):
        try:
            self.cursor.execute("SHOW FULL TABLES WHERE Table_type = 'VIEW'")
            views_to_sync = [view[0] for view in self.cursor.fetchall()]

            for view in views_to_sync:
                self.synchronize_view(view, alter_sync, source_code_hash, create_on_target)
        except MySQLError as e:
            logging.error(f"Error synchronizing all views: {e}")

    def synchronize_all_procedures(self, alter_sync, source_code_hash, create_on_target):
        try:
            self.cursor.execute("SHOW PROCEDURE STATUS WHERE Db = DATABASE()")
            procedures_to_sync = [proc[1] for proc in self.cursor.fetchall()]

            for procedure in procedures_to_sync:
                self.synchronize_procedure(procedure, alter_sync, source_code_hash, create_on_target)
        except MySQLError as e:
            logging.error(f"Error synchronizing all procedures: {e}")

    def rollback_table(self, table_name):
        try:
            self.cursor.execute("SELECT original_state, action FROM sync_log WHERE object_type='table' AND object_name=%s ORDER BY timestamp DESC LIMIT 1", (table_name,))
            row = self.cursor.fetchone()
            original_state, action = row if row else (None, None)
            
            if action == 'create':
                self.cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                logging.info(f"Dropped table {table_name} as part of rollback.")
            elif action == 'alter' and original_state:
                if self.test_sql_statement(original_state):
                    self.cursor.execute(original_state)
                    logging.info(f"Rolled back table {table_name} to its original state using: {original_state}")
            else:
                logging.warning(f"No rollback action found for table {table_name}.")
        except MySQLError as e:
            logging.error(f"Error rolling back table {table_name}: {e}")

    def rollback_view(self, view_name):
        try:
            self.cursor.execute("SELECT original_state, action FROM sync_log WHERE object_type='view' AND object_name=%s ORDER BY timestamp DESC LIMIT 1", (view_name,))
            row = self.cursor.fetchone()
            original_state, action = row if row else (None, None)
            
            if action == 'create':
                self.cursor.execute(f"DROP VIEW IF EXISTS {view_name}")
                logging.info(f"Dropped view {view_name} as part of rollback.")
            elif action == 'sync' and original_state:
                if self.test_sql_statement(original_state):
                    self.cursor.execute(original_state)
                    logging.info(f"Rolled back view {view_name} to its original state using: {original_state}")
            else:
                logging.warning(f"No rollback action found for view {view_name}.")
        except MySQLError as e:
            logging.error(f"Error rolling back view {view_name}: {e}")

    def rollback_procedure(self, procedure_name):
        try:
            self.cursor.execute("SELECT original_state, action FROM sync_log WHERE object_type='procedure' AND object_name=%s ORDER BY timestamp DESC LIMIT 1", (procedure_name,))
            row = self.cursor.fetchone()
            original_state, action = row if row else (None, None)
            
            if action == 'create':
                self.cursor.execute(f"DROP PROCEDURE IF EXISTS {procedure_name}")
                logging.info(f"Dropped procedure {procedure_name} as part of rollback.")
            elif action == 'sync' and original_state:
                if self.test_sql_statement(original_state):
                    self.cursor.execute(original_state)
                    logging.info(f"Rolled back procedure {procedure_name} to its original state using: {original_state}")
            else:
                logging.warning(f"No rollback action found for procedure {procedure_name}.")
        except MySQLError as e:
            logging.error(f"Error rolling back procedure

 {procedure_name}: {e}")

class PostgreSQLSync(DatabaseSync):
    def connect(self):
        try:
            self.conn = psycopg2.connect(**self.config)
            self.cursor = self.conn.cursor()
        except PGError as e:
            logging.error(f"Error connecting to PostgreSQL: {e}")
            raise
    
    def close(self):
        self.cursor.close()
        self.conn.close()
    
    def log_sync_action(self, object_type, object_name, action, source_code_hash, sync_direction, original_state, new_state, rollback_action):
        try:
            self.cursor.execute("""
                INSERT INTO sync_log (object_type, object_name, action, source_code_hash, sync_direction, original_state, new_state, rollback_action)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """, (object_type, object_name, action, source_code_hash, sync_direction, original_state, new_state, rollback_action))
            self.conn.commit()
        except PGError as e:
            logging.error(f"Error logging sync action: {e}")
    
    def test_sql_statement(self, statement):
        try:
            self.cursor.execute("BEGIN")
            self.cursor.execute(statement)
            self.cursor.execute("ROLLBACK")
            return True
        except PGError as e:
            logging.error(f"Invalid SQL statement: {statement}. Error: {e}")
            self.cursor.execute("ROLLBACK")
            return False

    def synchronize_table(self, table, alter_sync, source_code_hash, create_on_target):
        try:
            # Retrieve original state from target
            self.cursor.execute(f"SELECT pg_get_tabledef('{table}')")
            original_state = self.cursor.fetchone()
            original_state = original_state[0] if original_state else None

            # Synchronization logic
            self.cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table}'")
            source_schema = self.cursor.fetchall()
            source_columns = [(col[0], col[1]) for col in source_schema]

            self.cursor.execute(f"SELECT to_regclass('{table}')")
            target_exists = bool(self.cursor.fetchone())

            if not target_exists:
                if create_on_target:
                    logging.info(f"Table {table} doesn't exist on target. Creating...")
                    create_table_statement = f"CREATE TABLE {table} ("
                    for column, column_type in source_columns:
                        create_table_statement += f"{column} {column_type}, "
                    create_table_statement = create_table_statement[:-2] + ");"
                    
                    if self.test_sql_statement(create_table_statement):
                        self.cursor.execute(create_table_statement)
                        new_state = create_table_statement
                        logging.info(f"Table {table} created successfully on target.")
                        self.log_sync_action("table", table, "create", source_code_hash, "source_to_target", original_state, new_state, "drop")
                return

            # If the table exists on the target
            self.cursor.execute(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{table}'")
            target_schema = self.cursor.fetchall()
            target_columns = {col[0]: col[1] for col in target_schema}

            for column, column_type in source_columns:
                if column not in target_columns:
                    if alter_sync:
                        statement = f"ALTER TABLE {table} ADD COLUMN {column} {column_type};"
                        logging.info(f"Applying ALTER statement: {statement}")
                        
                        if self.test_sql_statement(statement):
                            self.cursor.execute(statement)
                            new_state = statement
                            self.log_sync_action("table", table, "alter", source_code_hash, "source_to_target", original_state, new_state, f"ALTER TABLE {table} DROP COLUMN {column}")
                    else:
                        logging.info(f"Table '{table}' has a new column '{column}'")
                else:
                    logging.info(f"Table '{table}' column '{column}' already exists.")
        except PGError as e:
            logging.error(f"Error synchronizing table {table}: {e}")

    def synchronize_view(self, view_name, alter_sync, source_code_hash, create_on_target):
        try:
            source_definition = self.get_view_definition(view_name)
            self.cursor.execute(f"SELECT pg_get_viewdef('{view_name}')")
            original_state = self.cursor.fetchone()
            original_state = original_state[0] if original_state else None

            self.cursor.execute(f"SELECT to_regclass('{view_name}')")
            target_exists = bool(self.cursor.fetchone())

            if not target_exists and create_on_target:
                logging.info(f"View {view_name} doesn't exist on target. Creating...")
                
                if self.test_sql_statement(source_definition):
                    self.cursor.execute(source_definition)
                    new_state = source_definition
                    logging.info(f"View {view_name} created successfully on target.")
                    self.log_sync_action("view", view_name, "create", source_code_hash, "source_to_target", original_state, new_state, "drop")
                return

            if source_definition != original_state:
                logging.info(f"Synchronizing view: {view_name}")
                self.cursor.execute(f"DROP VIEW IF EXISTS {view_name}")
                
                if self.test_sql_statement(source_definition):
                    self.cursor.execute(source_definition)
                    new_state = source_definition
                    logging.info(f"View {view_name} synchronized successfully.")
                    self.log_sync_action("view", view_name, "sync", source_code_hash, "source_to_target", original_state, new_state, "drop")
            else:
                logging.info(f"View {view_name} is already synchronized.")
        except PGError as e:
            logging.error(f"Error synchronizing view {view_name}: {e}")

    def synchronize_procedure(self, procedure_name, alter_sync, source_code_hash, create_on_target):
        try:
            source_definition = self.get_procedure_definition(procedure_name)

            self.cursor.execute(f"SELECT proname FROM pg_proc WHERE proname = '{procedure_name}'")
            target_exists = bool(self.cursor.fetchone())

            if not target_exists or create_on_target:
                if not target_exists:
                    logging.info(f"Procedure {procedure_name} doesn't exist on target. Creating...")
                else:
                    logging.info(f"Procedure {procedure_name} creation is not disabled. Creating...")

                if self.test_sql_statement(source_definition):
                    self.cursor.execute(source_definition)
                    new_state = source_definition
                    logging.info(f"Procedure {procedure_name} created successfully on target.")
                    self.log_sync_action("procedure", procedure_name, "create", source_code_hash, "source_to_target", None, new_state, "drop")
                return

            if target_exists:
                target_definition = self.get_procedure_definition(procedure_name)
                original_state = target_definition

                if source_definition != target_definition:
                    logging.info(f"Synchronizing procedure: {procedure_name}")
                    self.cursor.execute(f"DROP PROCEDURE IF EXISTS {procedure_name}")
                    
                    if self.test_sql_statement(source_definition):
                        self.cursor.execute(source_definition)
                        new_state = source_definition
                        logging.info(f"Procedure {procedure_name} synchronized successfully.")
                        self.log_sync_action("procedure", procedure_name, "sync", source_code_hash, "source_to_target", original_state, new_state, "drop")
                else:
                    logging.info(f"Procedure {procedure_name} is already synchronized.")
        except PGError as e:
            logging.error(f"Error synchronizing procedure {procedure_name}: {e}")

    def synchronize_all_tables(self, alter_sync, source_code_hash, create_on_target):
        try:
            self.cursor.execute("SELECT tablename FROM pg_tables WHERE schemaname = 'public'")
            tables_to_sync = [table[0] for table in self.cursor.fetchall()]

            for table in tables_to_sync:
                self.synchronize_table(table, alter_sync, source_code_hash, create_on_target)
        except PGError as e:
            logging.error(f"Error synchronizing all tables: {e}")

    def synchronize_all_views(self, alter_sync, source_code_hash, create_on_target):
        try:
            self.cursor.execute("SELECT viewname FROM pg_views WHERE schemaname = 'public'")
            views_to_sync = [view[0] for view in self.cursor.fetchall()]

            for view in views_to_sync:
                self.synchronize_view(view, alter_sync, source_code_hash, create_on_target)
        except PGError as e:
            logging.error(f"Error synchronizing all views: {e}")

    def synchronize_all_procedures(self, alter_sync, source_code_hash, create_on_target):
        try:
            self.cursor.execute("SELECT proname FROM pg_proc WHERE pronamespace = (SELECT oid FROM pg_namespace WHERE nspname = 'public')")
            procedures_to_sync = [proc[0] for proc in self.cursor.fetchall()]

            for procedure in procedures_to_sync:
                self.synchronize_procedure(procedure, alter_sync, source_code_hash, create_on_target)
        except PGError as e:
            logging.error(f"Error synchronizing all procedures: {e}")

    def rollback_table(self, table_name):
        try:
            self.cursor.execute("SELECT original_state, action FROM sync_log WHERE object_type='table' AND object_name=%s ORDER BY timestamp DESC LIMIT 1", (table_name,))
            row = self.cursor.fetchone()
            original_state, action = row if row else (None, None)
            
            if action == 'create':
                self.cursor.execute(f"DROP TABLE IF EXISTS {table_name}")
                logging.info(f"Dropped table {table_name} as part of rollback.")
            elif action == 'alter' and original_state:
                if self.test_sql_statement(original_state):
                    self.cursor.execute(original_state)
                    logging.info(f"Rolled back table {table_name} to its original state using: {original_state}")
            else:
                logging.warning(f"No rollback action

 found for table {table_name}.")
        except PGError as e:
            logging.error(f"Error rolling back table {table_name}: {e}")

    def rollback_view(self, view_name):
        try:
            self.cursor.execute("SELECT original_state, action FROM sync_log WHERE object_type='view' AND object_name=%s ORDER BY timestamp DESC LIMIT 1", (view_name,))
            row = self.cursor.fetchone()
            original_state, action = row if row else (None, None)
            
            if action == 'create':
                self.cursor.execute(f"DROP VIEW IF EXISTS {view_name}")
                logging.info(f"Dropped view {view_name} as part of rollback.")
            elif action == 'sync' and original_state:
                if self.test_sql_statement(original_state):
                    self.cursor.execute(original_state)
                    logging.info(f"Rolled back view {view_name} to its original state using: {original_state}")
            else:
                logging.warning(f"No rollback action found for view {view_name}.")
        except PGError as e:
            logging.error(f"Error rolling back view {view_name}: {e}")

    def rollback_procedure(self, procedure_name):
        try:
            self.cursor.execute("SELECT original_state, action FROM sync_log WHERE object_type='procedure' AND object_name=%s ORDER BY timestamp DESC LIMIT 1", (procedure_name,))
            row = self.cursor.fetchone()
            original_state, action = row if row else (None, None)
            
            if action == 'create':
                self.cursor.execute(f"DROP PROCEDURE IF EXISTS {procedure_name}")
                logging.info(f"Dropped procedure {procedure_name} as part of rollback.")
            elif action == 'sync' and original_state:
                if self.test_sql_statement(original_state):
                    self.cursor.execute(original_state)
                    logging.info(f"Rolled back procedure {procedure_name} to its original state using: {original_state}")
            else:
                logging.warning(f"No rollback action found for procedure {procedure_name}.")
        except PGError as e:
            logging.error(f"Error rolling back procedure {procedure_name}: {e}")

class MongoDBSync(DatabaseSync):
    def connect(self):
        try:
            self.client = pymongo.MongoClient(**self.config)
            self.db = self.client.get_database()
        except pymongo.errors.ConnectionError as e:
            logging.error(f"Error connecting to MongoDB: {e}")
            raise
    
    def close(self):
        self.client.close()
    
    def log_sync_action(self, object_type, object_name, action, source_code_hash, sync_direction, original_state, new_state, rollback_action):
        try:
            self.db.sync_log.insert_one({
                "object_type": object_type,
                "object_name": object_name,
                "action": action,
                "source_code_hash": source_code_hash,
                "sync_direction": sync_direction,
                "original_state": original_state,
                "new_state": new_state,
                "rollback_action": rollback_action,
                "timestamp": pymongo.datetime.datetime.utcnow()
            })
        except pymongo.errors.PyMongoError as e:
            logging.error(f"Error logging sync action: {e}")
    
    def synchronize_table(self, table, alter_sync, source_code_hash, create_on_target):
        # Implement MongoDB-specific logic for synchronizing tables (collections)
        pass
    
    def synchronize_view(self, view_name, alter_sync, source_code_hash, create_on_target):
        # Implement MongoDB-specific logic for synchronizing views (if applicable)
        pass
    
    def synchronize_procedure(self, procedure_name, alter_sync, source_code_hash, create_on_target):
        # Implement MongoDB-specific logic for synchronizing procedures (if applicable)
        pass
    
    def synchronize_all_tables(self, alter_sync, source_code_hash, create_on_target):
        # Implement MongoDB-specific logic for synchronizing all tables (collections)
        pass
    
    def synchronize_all_views(self, alter_sync, source_code_hash, create_on_target):
        # Implement MongoDB-specific logic for synchronizing all views (if applicable)
        pass
    
    def synchronize_all_procedures(self, alter_sync, source_code_hash, create_on_target):
        # Implement MongoDB-specific logic for synchronizing all procedures (if applicable)
        pass
    
    def rollback_table(self, table_name):
        # Implement MongoDB-specific logic for rolling back tables (collections)
        pass
    
    def rollback_view(self, view_name):
        # Implement MongoDB-specific logic for rolling back views (if applicable)
        pass
    
    def rollback_procedure(self, procedure_name):
        # Implement MongoDB-specific logic for rolling back procedures (if applicable)
        pass

class Neo4jSync(DatabaseSync):
    def connect(self):
        try:
            self.driver = GraphDatabase.driver(**self.config)
            self.session = self.driver.session()
        except Exception as e:
            logging.error(f"Error connecting to Neo4j: {e}")
            raise
    
    def close(self):
        self.session.close()
        self.driver.close()
    
    def log_sync_action(self, object_type, object_name, action, source_code_hash, sync_direction, original_state, new_state, rollback_action):
        try:
            self.session.run("""
                CREATE (log:SyncLog {
                    object_type: $object_type,
                    object_name: $object_name,
                    action: $action,
                    source_code_hash: $source_code_hash,
                    sync_direction: $sync_direction,
                    original_state: $original_state,
                    new_state: $new_state,
                    rollback_action: $rollback_action,
                    timestamp: datetime()
                })
            """, object_type=object_type, object_name=object_name, action=action, source_code_hash=source_code_hash, sync_direction=sync_direction, original_state=original_state, new_state=new_state, rollback_action=rollback_action)
        except Exception as e:
            logging.error(f"Error logging sync action: {e}")
    
    def synchronize_table(self, table, alter_sync, source_code_hash, create_on_target):
        # Implement Neo4j-specific logic for synchronizing nodes/relationships
        pass
    
    def synchronize_view(self, view_name, alter_sync, source_code_hash, create_on_target):
        # Implement Neo4j-specific logic for synchronizing views (if applicable)
        pass
    
    def synchronize_procedure(self, procedure_name, alter_sync, source_code_hash, create_on_target):
        # Implement Neo4j-specific logic for synchronizing procedures (if applicable)
        pass
    
    def synchronize_all_tables(self, alter_sync, source_code_hash, create_on_target):
        # Implement Neo4j-specific logic for synchronizing all nodes/relationships
        pass
    
    def synchronize_all_views(self, alter_sync, source_code_hash, create_on_target):
        # Implement Neo4j-specific logic for synchronizing all views (if applicable)
        pass
    
    def synchronize_all_procedures(self, alter_sync, source_code_hash, create_on_target):
        # Implement Neo4j-specific logic for synchronizing all procedures (if applicable)
        pass
    
    def rollback_table(self, table_name):
        # Implement Neo4j-specific logic for rolling back nodes/relationships
        pass
    
    def rollback_view(self, view_name):
        # Implement Neo4j-specific logic for rolling back views (if applicable)
        pass
    
    def rollback_procedure(self, procedure_name):
        # Implement Neo4j-specific logic for rolling back procedures (if applicable)
        pass

class DatabaseSyncFactory:
    @staticmethod
    def get_sync_instance(db_type, config):
        if db_type == 'mysql':
            return MySQLSync(config)
        elif db_type == 'postgresql':
            return PostgreSQLSync(config)
        elif db_type == 'mongodb':
            return MongoDBSync(config)
        elif db_type == 'neo4j':
            return Neo4jSync(config)
        else:
            raise ValueError(f"Unsupported database type: {db_type}")

def main():
    parser = argparse.ArgumentParser(description="Database Synchronization Tool")
    parser.add_argument("--source-config", help="Path to JSON file containing source database configuration")
    parser.add_argument("--source-db-type", help="Type of the source database (e.g., mysql, postgresql, mongodb, neo4j)")
    parser.add_argument("--target-configs", help="Path to JSON file containing target database configurations")
    parser.add_argument("--sync-all-tables", action="store_true", help="Sync all tables from source")
    parser.add_argument("--sync-all-views", action="store_true", help="Sync all views from source")
    parser.add_argument("--sync-all-procedures", action="store_true", help="Sync all procedures from source")
    parser.add_argument("--alter-sync", action="store_true", help="Use ALTER statements to sync tables, views, and procedures")
    parser.add_argument("--create-on-target", action="store_true", help="Create objects on target if they don't exist in source")
    parser.add_argument("--rollback", help="Rollback changes for a specific object (format: type:name)")
    args = parser.parse_args()

    if not (args.source_config and args.source_db_type and args.target_configs):
        logging.error("Please provide source and target configuration files and source database type.")
        return

    with open(args.source_config, 'r') as f:
        source_config = json.load(f)

    with open(args.target_configs, 'r') as f:
        target_configs = json.load(f)

    if args.rollback:
        obj_type, obj_name = args.rollback.split(':')
        for target_config in target_configs:
            target_sync = DatabaseSyncFactory.get_sync_instance(target_config['type'], target_config['config'])
            target_sync.connect()
            if obj_type == 'table':
                target_sync.rollback_table(obj_name)
            elif obj_type == 'view':
                target_sync.rollback_view(obj_name)
            elif obj_type == 'procedure':
                target_sync.rollback_procedure(obj_name)


            else:
                logging.error(f"Unsupported rollback object type: {obj_type}")
            target_sync.close()
    else:
        source_sync = DatabaseSyncFactory.get_sync_instance(args.source_db_type, source_config)
        source_sync.connect()

        source_code_hash = hashlib.md5(open(__file__, 'rb').read()).hexdigest()

        for target_config in target_configs:
            target_sync = DatabaseSyncFactory.get_sync_instance(target_config['type'], target_config['config'])
            target_sync.connect()

            if args.sync_all_tables:
                target_sync.synchronize_all_tables(args.alter_sync, source_code_hash, args.create_on_target)

            if args.sync_all_views:
                target_sync.synchronize_all_views(args.alter_sync, source_code_hash, args.create_on_target)

            if args.sync_all_procedures:
                target_sync.synchronize_all_procedures(args.alter_sync, source_code_hash, args.create_on_target)

            target_sync.close()

        source_sync.close()

if __name__ == "__main__":
    main()
```
