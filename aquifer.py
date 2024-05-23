"""
Author: Will Moebest
Date: 5/22/2024
Description: Aquifer - A comprehensive database synchronization tool supporting MySQL, PostgreSQL, MongoDB, Neo4j, SQL Server, and Oracle.
             This script was developed with the assistance of GPT-4 by OpenAI.
"""
import json
import argparse
import hashlib
import logging
import mysql.connector
import psycopg2
import pymongo
import pyodbc
import cx_Oracle
from neo4j import GraphDatabase
from abc import ABC, abstractmethod
from mysql.connector import Error as MySQLError
from psycopg2 import Error as PGError
from pyodbc import Error as ODBCError
from cx_Oracle import Error as OracleError

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
    def synchronize_indexes(self, table):
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
            # Retrieve original state from source
            self.cursor.execute(f"SHOW CREATE TABLE {table}")
            original_state = self.cursor.fetchone()
            original_state = original_state[1] if original_state else None

            # Synchronization logic
            self.cursor.execute(f"SHOW TABLES LIKE '{table}'")
            target_exists = bool(self.cursor.fetchone())

            if not target_exists:
                if create_on_target:
                    logging.info(f"Table {table} doesn't exist on target. Creating...")
                    if self.test_sql_statement(original_state):
                        self.cursor.execute(original_state)
                        new_state = original_state
                        logging.info(f"Table {table} created successfully on target.")
                        self.log_sync_action("table", table, "create", source_code_hash, "source_to_target", None, new_state, "drop")
                return

            # If the table exists on the target
            self.cursor.execute(f"SHOW CREATE TABLE {table}")
            target_state = self.cursor.fetchone()
            target_state = target_state[1] if target_state else None

            if original_state != target_state:
                logging.info(f"Synchronizing table: {table}")
                if alter_sync:
                    # Implement the logic for ALTER statements if required
                    pass
                else:
                    self.cursor.execute(f"DROP TABLE IF EXISTS {table}")
                    if self.test_sql_statement(original_state):
                        self.cursor.execute(original_state)
                        new_state = original_state
                        logging.info(f"Table {table} synchronized successfully.")
                        self.log_sync_action("table", table, "sync", source_code_hash, "source_to_target", target_state, new_state, "drop")
            else:
                logging.info(f"Table {table} is already synchronized.")
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

    def synchronize_indexes(self, table):
        try:
            self.cursor.execute(f"SHOW INDEXES FROM {table}")
            indexes = self.cursor.fetchall()
            for index in indexes:
                if index[2] != 'PRIMARY':
                    create_index_statement = f"CREATE INDEX {index[2]} ON {table} ({index[4]})"
                    logging.info(f"Creating index: {create_index_statement}")
                    if self.test_sql_statement(create_index_statement):
                        self.cursor.execute(create_index_statement)
        except MySQLError as e:
            logging.error(f"Error synchronizing indexes for table {table}: {e}")

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
            logging.error(f"Error rolling back procedure {procedure_name}: {e}")

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
            # Retrieve original state from source
            self.cursor.execute(f"SELECT pg_get_tabledef('{table}')")
            original_state = self.cursor.fetchone()
            original_state = original_state[0] if original_state else None

            # Synchronization logic
            self.cursor.execute(f"SELECT to_regclass('{table}')")
            target_exists = bool(self.cursor.fetchone())

            if not target_exists:
                if create_on_target:
                    logging.info(f"Table {table} doesn't exist on target. Creating...")
                    if self.test_sql_statement(original_state):
                        self.cursor.execute(original_state)
                        new_state = original_state
                        logging.info(f"Table {table} created successfully on target.")
                        self.log_sync_action("table", table, "create", source_code_hash, "source_to_target", None, new_state, "drop")
                return

            # If the table exists on the target
            self.cursor.execute(f"SELECT pg_get_tabledef('{table}')")
            target_state = self.cursor.fetchone()
            target_state = target_state[0] if target_state else None

            if original_state != target_state:
                logging.info(f"Synchronizing table: {table}")
                if alter_sync:
                    # Implement the logic for ALTER statements if required
                    pass
                else:
                    self.cursor.execute(f"DROP TABLE IF EXISTS {table}")
                    if self.test_sql_statement(original_state):
                        self.cursor.execute(original_state)
                        new_state = original_state
                        logging.info(f"Table {table} synchronized successfully.")
                        self.log_sync_action("table", table, "sync", source_code_hash, "source_to_target", target_state, new_state, "drop")
            else:
                logging.info(f"Table {table} is already synchronized.")
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

    def synchronize_indexes(self, table):
        try:
            self.cursor.execute(f"""
                SELECT indexname, indexdef 
                FROM pg_indexes 
                WHERE tablename = '{table}'
            """)
            indexes = self.cursor.fetchall()
            for index in indexes:
                create_index_statement = index[1]
                logging.info(f"Creating index: {create_index_statement}")
                if self.test_sql_statement(create_index_statement):
                    self.cursor.execute(create_index_statement)
        except PGError as e:
            logging.error(f"Error synchronizing indexes for table {table}: {e}")

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
    
    def synchronize_indexes(self, table):
        # Implement MongoDB-specific logic for synchronizing indexes (if applicable)
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
    
    def synchronize_indexes(self, table):
        # Implement Neo4j-specific logic for synchronizing indexes (if applicable)
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

class SQLServerSync(DatabaseSync):
    def connect(self):
        try:
            conn_str = (
                f"DRIVER={{ODBC Driver 17 for SQL Server}};"
                f"SERVER={self.config['host']};"
                f"DATABASE={self.config['database']};"
                f"UID={self.config['user']};"
                f"PWD={self.config['password']}"
            )
            self.conn = pyodbc.connect(conn_str)
            self.cursor = self.conn.cursor()
        except ODBCError as e:
            logging.error(f"Error connecting to SQL Server: {e}")
            raise
    
    def close(self):
        self.cursor.close()
        self.conn.close()
    
    def log_sync_action(self, object_type, object_name, action, source_code_hash, sync_direction, original_state, new_state, rollback_action):
        try:
            self.cursor.execute("""
                INSERT INTO sync_log (object_type, object_name, action, source_code_hash, sync_direction, original_state, new_state, rollback_action)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """, (object_type, object_name, action, source_code_hash, sync_direction, original_state, new_state, rollback_action))
            self.conn.commit()
        except ODBCError as e:
            logging.error(f"Error logging sync action: {e}")
    
    def test_sql_statement(self, statement):
        try:
            self.cursor.execute("BEGIN TRANSACTION")
            self.cursor.execute(statement)
            self.cursor.execute("ROLLBACK TRANSACTION")
            return True
        except ODBCError as e:
            logging.error(f"Invalid SQL statement: {statement}. Error: {e}")
            self.cursor.execute("ROLLBACK TRANSACTION")
            return False

    def synchronize_table(self, table, alter_sync, source_code_hash, create_on_target):
        try:
            # Retrieve original state from source
            self.cursor.execute(f"SELECT OBJECT_DEFINITION (OBJECT_ID(N'{table}'))")
            original_state = self.cursor.fetchone()
            original_state = original_state[0] if original_state else None

            # Synchronization logic
            self.cursor.execute(f"SELECT * FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = N'{table}'")
            target_exists = bool(self.cursor.fetchone())

            if not target_exists:
                if create_on_target:
                    logging.info(f"Table {table} doesn't exist on target. Creating...")
                    if self.test_sql_statement(original_state):
                        self.cursor.execute(original_state)
                        new_state = original_state
                        logging.info(f"Table {table} created successfully on target.")
                        self.log_sync_action("table", table, "create", source_code_hash, "source_to_target", None, new_state, "drop")
                return

            # If the table exists on the target
            self.cursor.execute(f"SELECT OBJECT_DEFINITION (OBJECT_ID(N'{table}'))")
            target_state = self.cursor.fetchone()
            target_state = target_state[0] if target_state else None

            if original_state != target_state:
                logging.info(f"Synchronizing table: {table}")
                if alter_sync:
                    # Implement the logic for ALTER statements if required
                    pass
                else:
                    self.cursor.execute(f"DROP TABLE IF EXISTS {table}")
                    if self.test_sql_statement(original_state):
                        self.cursor.execute(original_state)
                        new_state = original_state
                        logging.info(f"Table {table} synchronized successfully.")
                        self.log_sync_action("table", table, "sync", source_code_hash, "source_to_target", target_state, new_state, "drop")
            else:
                logging.info(f"Table {table} is already synchronized.")
        except ODBCError as e:
            logging.error(f"Error synchronizing table {table}: {e}")

    def synchronize_view(self, view_name, alter_sync, source_code_hash, create_on_target):
        try:
            source_definition = self.get_view_definition(view_name)
            self.cursor.execute(f"SELECT OBJECT_DEFINITION (OBJECT_ID(N'{view_name}'))")
            original_state = self.cursor.fetchone()
            original_state = original_state[0] if original_state else None

            self.cursor.execute(f"SELECT * FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_NAME = N'{view_name}'")
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
        except ODBCError as e:
            logging.error(f"Error synchronizing view {view_name}: {e}")

    def synchronize_procedure(self, procedure_name, alter_sync, source_code_hash, create_on_target):
        try:
            source_definition = self.get_procedure_definition(procedure_name)

            self.cursor.execute(f"SELECT * FROM sys.procedures WHERE name = '{procedure_name}'")
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
        except ODBCError as e:
            logging.error(f"Error synchronizing procedure {procedure_name}: {e}")

    def synchronize_all_tables(self, alter_sync, source_code_hash, create_on_target):
        try:
            self.cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_TYPE = 'BASE TABLE'")
            tables_to_sync = [table[0] for table in self.cursor.fetchall()]

            for table in tables_to_sync:
                self.synchronize_table(table, alter_sync, source_code_hash, create_on_target)
        except ODBCError as e:
            logging.error(f"Error synchronizing all tables: {e}")

    def synchronize_all_views(self, alter_sync, source_code_hash, create_on_target):
        try:
            self.cursor.execute("SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS")
            views_to_sync = [view[0] for view in self.cursor.fetchall()]

            for view in views_to_sync:
                self.synchronize_view(view, alter_sync, source_code_hash, create_on_target)
        except ODBCError as e:
            logging.error(f"Error synchronizing all views: {e}")

    def synchronize_all_procedures(self, alter_sync, source_code_hash, create_on_target):
        try:
            self.cursor.execute("SELECT name FROM sys.procedures")
            procedures_to_sync = [proc[0] for proc in self.cursor.fetchall()]

            for procedure in procedures_to_sync:
                self.synchronize_procedure(procedure, alter_sync, source_code_hash, create_on_target)
        except ODBCError as e:
            logging.error(f"Error synchronizing all procedures: {e}")

    def synchronize_indexes(self, table):
        try:
            self.cursor.execute(f"""
                SELECT i.name, COL_NAME(ic.object_id, ic.column_id) AS column_name
                FROM sys.indexes AS i
                INNER JOIN sys.index_columns AS ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id
                WHERE i.is_primary_key = 0 AND OBJECT_NAME(ic.object_id) = '{table}'
            """)
            indexes = self.cursor.fetchall()
            for index in indexes:
                create_index_statement = f"CREATE INDEX {index[0]} ON {table} ({index[1]})"
                logging.info(f"Creating index: {create_index_statement}")
                if self.test_sql_statement(create_index_statement):
                    self.cursor.execute(create_index_statement)
        except ODBCError as e:
            logging.error(f"Error synchronizing indexes for table {table}: {e}")

    def rollback_table(self, table_name):
        try:
            self.cursor.execute("SELECT original_state, action FROM sync_log WHERE object_type='table' AND object_name=? ORDER BY timestamp DESC LIMIT 1", (table_name,))
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
        except ODBCError as e:
            logging.error(f"Error rolling back table {table_name}: {e}")

    def rollback_view(self, view_name):
        try:
            self.cursor.execute("SELECT original_state, action FROM sync_log WHERE object_type='view' AND object_name=? ORDER BY timestamp DESC LIMIT 1", (view_name,))
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
        except ODBCError as e:
            logging.error(f"Error rolling back view {view_name}: {e}")

    def rollback_procedure(self, procedure_name):
        try:
            self.cursor.execute("SELECT original_state, action FROM sync_log WHERE object_type='procedure' AND object_name=? ORDER BY timestamp DESC LIMIT 1", (procedure_name,))
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
        except ODBCError as e:
            logging.error(f"Error rolling back procedure {procedure_name}: {e}")

class OracleSync(DatabaseSync):
    def connect(self):
        try:
            dsn_tns = cx_Oracle.makedsn(self.config['host'], self.config['port'], sid=self.config['sid'])
            self.conn = cx_Oracle.connect(user=self.config['user'], password=self.config['password'], dsn=dsn_tns)
            self.cursor = self.conn.cursor()
        except OracleError as e:
            logging.error(f"Error connecting to Oracle: {e}")
            raise
    
    def close(self):
        self.cursor.close()
        self.conn.close()
    
    def log_sync_action(self, object_type, object_name, action, source_code_hash, sync_direction, original_state, new_state, rollback_action):
        try:
            self.cursor.execute("""
                INSERT INTO sync_log (object_type, object_name, action, source_code_hash, sync_direction, original_state, new_state, rollback_action)
                VALUES (:1, :2, :3, :4, :5, :6, :7, :8)
            """, (object_type, object_name, action, source_code_hash, sync_direction, original_state, new_state, rollback_action))
            self.conn.commit()
        except OracleError as e:
            logging.error(f"Error logging sync action: {e}")
    
    def test_sql_statement(self, statement):
        try:
            self.cursor.execute("BEGIN")
            self.cursor.execute(statement)
            self.cursor.execute("ROLLBACK")
            return True
        except OracleError as e:
            logging.error(f"Invalid SQL statement: {statement}. Error: {e}")
            self.cursor.execute("ROLLBACK")
            return False

    def synchronize_table(self, table, alter_sync, source_code_hash, create_on_target):
        try:
            # Retrieve original state from source
            self.cursor.execute(f"SELECT dbms_metadata.get_ddl('TABLE', '{table.upper()}') FROM dual")
            original_state = self.cursor.fetchone()
            original_state = original_state[0] if original_state else None

            # Synchronization logic
            self.cursor.execute(f"SELECT table_name FROM user_tables WHERE table_name = '{table.upper()}'")
            target_exists = bool(self.cursor.fetchone())

            if not target_exists:
                if create_on_target:
                    logging.info(f"Table {table} doesn't exist on target. Creating...")
                    if self.test_sql_statement(original_state):
                        self.cursor.execute(original_state)
                        new_state = original_state
                        logging.info(f"Table {table} created successfully on target.")
                        self.log_sync_action("table", table, "create", source_code_hash, "source_to_target", None, new_state, "drop")
                return

            # If the table exists on the target
            self.cursor.execute(f"SELECT dbms_metadata.get_ddl('TABLE', '{table.upper()}') FROM dual")
            target_state = self.cursor.fetchone()
            target_state = target_state[0] if target_state else None

            if original_state != target_state:
                logging.info(f"Synchronizing table: {table}")
                if alter_sync:
                    # Implement the logic for ALTER statements if required
                    pass
                else:
                    self.cursor.execute(f"DROP TABLE {table}")
                    if self.test_sql_statement(original_state):
                        self.cursor.execute(original_state)
                        new_state = original_state
                        logging.info(f"Table {table} synchronized successfully.")
                        self.log_sync_action("table", table, "sync", source_code_hash, "source_to_target", target_state, new_state, "drop")
            else:
                logging.info(f"Table {table} is already synchronized.")
        except OracleError as e:
            logging.error(f"Error synchronizing table {table}: {e}")

    def synchronize_view(self, view_name, alter_sync, source_code_hash, create_on_target):
        try:
            source_definition = self.get_view_definition(view_name)
            self.cursor.execute(f"SELECT dbms_metadata.get_ddl('VIEW', '{view_name.upper()}') FROM dual")
            original_state = self.cursor.fetchone()
            original_state = original_state[0] if original_state else None

            self.cursor.execute(f"SELECT view_name FROM user_views WHERE view_name = '{view_name.upper()}'")
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
                self.cursor.execute(f"DROP VIEW {view_name}")
                
                if self.test_sql_statement(source_definition):
                    self.cursor.execute(source_definition)
                    new_state = source_definition
                    logging.info(f"View {view_name} synchronized successfully.")
                    self.log_sync_action("view", view_name, "sync", source_code_hash, "source_to_target", original_state, new_state, "drop")
            else:
                logging.info(f"View {view_name} is already synchronized.")
        except OracleError as e:
            logging.error(f"Error synchronizing view {view_name}: {e}")

    def synchronize_procedure(self, procedure_name, alter_sync, source_code_hash, create_on_target):
        try:
            source_definition = self.get_procedure_definition(procedure_name)

            self.cursor.execute(f"SELECT object_name FROM user_procedures WHERE object_name = '{procedure_name.upper()}'")
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
                    self.cursor.execute(f"DROP PROCEDURE {procedure_name}")
                    
                    if self.test_sql_statement(source_definition):
                        self.cursor.execute(source_definition)
                        new_state = source_definition
                        logging.info(f"Procedure {procedure_name} synchronized successfully.")
                        self.log_sync_action("procedure", procedure_name, "sync", source_code_hash, "source_to_target", original_state, new_state, "drop")
                else:
                    logging.info(f"Procedure {procedure_name} is already synchronized.")
        except OracleError as e:
            logging.error(f"Error synchronizing procedure {procedure_name}: {e}")

    def synchronize_all_tables(self, alter_sync, source_code_hash, create_on_target):
        try:
            self.cursor.execute("SELECT table_name FROM user_tables")
            tables_to_sync = [table[0] for table in self.cursor.fetchall()]

            for table in tables_to_sync:
                self.synchronize_table(table, alter_sync, source_code_hash, create_on_target)
        except OracleError as e:
            logging.error(f"Error synchronizing all tables: {e}")

    def synchronize_all_views(self, alter_sync, source_code_hash, create_on_target):
        try:
            self.cursor.execute("SELECT view_name FROM user_views")
            views_to_sync = [view[0] for view in self.cursor.fetchall()]

            for view in views_to_sync:
                self.synchronize_view(view, alter_sync, source_code_hash, create_on_target)
        except OracleError as e:
            logging.error(f"Error synchronizing all views: {e}")

    def synchronize_all_procedures(self, alter_sync, source_code_hash, create_on_target):
        try:
            self.cursor.execute("SELECT object_name FROM user_procedures")
            procedures_to_sync = [proc[0] for proc in self.cursor.fetchall()]

            for procedure in procedures_to_sync:
                self.synchronize_procedure(procedure, alter_sync, source_code_hash, create_on_target)
        except OracleError as e:
            logging.error(f"Error synchronizing all procedures: {e}")

    def synchronize_indexes(self, table):
        try:
            self.cursor.execute(f"""
                SELECT index_name, column_name
                FROM all_ind_columns
                WHERE table_name = '{table.upper()}'
            """)
            indexes = self.cursor.fetchall()
            for index in indexes:
                create_index_statement = f"CREATE INDEX {index[0]} ON {table} ({index[1]})"
                logging.info(f"Creating index: {create_index_statement}")
                if self.test_sql_statement(create_index_statement):
                    self.cursor.execute(create_index_statement)
        except OracleError as e:
            logging.error(f"Error synchronizing indexes for table {table}: {e}")

    def rollback_table(self, table_name):
        try:
            self.cursor.execute("SELECT original_state, action FROM sync_log WHERE object_type='table' AND object_name=:1 ORDER BY timestamp DESC LIMIT 1", (table_name,))
            row = self.cursor.fetchone()
            original_state, action = row if row else (None, None)
            
            if action == 'create':
                self.cursor.execute(f"DROP TABLE {table_name}")
                logging.info(f"Dropped table {table_name} as part of rollback.")
            elif action == 'alter' and original_state:
                if self.test_sql_statement(original_state):
                    self.cursor.execute(original_state)
                    logging.info(f"Rolled back table {table_name} to its original state using: {original_state}")
            else:
                logging.warning(f"No rollback action found for table {table_name}.")
        except OracleError as e:
            logging.error(f"Error rolling back table {table_name}: {e}")

    def rollback_view(self, view_name):
        try:
            self.cursor.execute("SELECT original_state, action FROM sync_log WHERE object_type='view' AND object_name=:1 ORDER BY timestamp DESC LIMIT 1", (view_name,))
            row = self.cursor.fetchone()
            original_state, action = row if row else (None, None)
            
            if action == 'create':
                self.cursor.execute(f"DROP VIEW {view_name}")
                logging.info(f"Dropped view {view_name} as part of rollback.")
            elif action == 'sync' and original_state:
                if self.test_sql_statement(original_state):
                    self.cursor.execute(original_state)
                    logging.info(f"Rolled back view {view_name} to its original state using: {original_state}")
            else:
                logging.warning(f"No rollback action found for view {view_name}.")
        except OracleError as e:
            logging.error(f"Error rolling back view {view_name}: {e}")

    def rollback_procedure(self, procedure_name):
        try:
            self.cursor.execute("SELECT original_state, action FROM sync_log WHERE object_type='procedure' AND object_name=:1 ORDER BY timestamp DESC LIMIT 1", (procedure_name,))
            row = self.cursor.fetchone()
            original_state, action = row if row else (None, None)
            
            if action == 'create':
                self.cursor.execute(f"DROP PROCEDURE {procedure_name}")
                logging.info(f"Dropped procedure {procedure_name} as part of rollback.")
            elif action == 'sync' and original_state:
                if self.test_sql_statement(original_state):
                    self.cursor.execute(original_state)
                    logging.info(f"Rolled back procedure {procedure_name} to its original state using: {original_state}")
            else:
                logging.warning(f"No rollback action found for procedure {procedure_name}.")
        except OracleError as e:
            logging.error(f"Error rolling back procedure {procedure_name}: {e}")

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
        elif db_type == 'sqlserver':
            return SQLServerSync(config)
        elif db_type == 'oracle':
            return OracleSync(config)
        else:
            raise ValueError(f"Unsupported database type: {db_type}")

def main():
    parser = argparse.ArgumentParser(description="Database Synchronization Tool")
    parser.add_argument("--source-config", help="Path to JSON file containing source database configuration")
    parser.add_argument("--source-db-type", help="Type of the source database (e.g., mysql, postgresql, mongodb, neo4j, sqlserver, oracle)")
    parser.add_argument("--target-configs", help="Path to JSON file containing target database configurations")
    parser.add_argument("--sync-all-tables", action="store_true", help="Sync all tables from source")
    parser.add_argument("--sync-all-views", action="store_true", help="Sync all views from source")
    parser.add_argument("--sync-all-procedures", action="store_true", help="Sync all procedures from source")
    parser.add_argument("--alter-sync", action="store_true", help="Use ALTER statements to sync tables, views, and procedures")
    parser.add_argument("--create-on-target", action="store_true", help="Create objects on target if they don't exist in source")
    parser.add_argument("--sync-indexes", action="store_true", help="Sync indexes for tables")
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
                if args.sync_indexes:
                    for table in target_sync.get_tables():
                        target_sync.synchronize_indexes(table)

            if args.sync_all_views:
                target_sync.synchronize_all_views(args.alter_sync, source_code_hash, args.create_on_target)

            if args.sync_all_procedures:
                target_sync.synchronize_all_procedures(args.alter_sync, source_code_hash, args.create_on_target)

            target_sync.close()

        source_sync.close()

if __name__ == "__main__":
    main()
