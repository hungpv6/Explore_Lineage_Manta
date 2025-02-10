import os
import sys
from .connection import RepoConnection
import pandas as pd
import numpy as np
from pathlib import Path
sys.path.append(str(Path(os.path.abspath(__file__)).parents[3]))
from logger_module import setup_logger_global

class RepoOps(RepoConnection):
    def __init__(self):
        self.logger_database_ops = setup_logger_global(os.path.basename(__file__), os.path.basename(__file__) + '.log')

    def execute_query(self, connection, query, params=None):
        """
        Execute a SELECT query and return fetched results.
        """
        if not connection:
            self.logger_database_ops.warning("No active database connection. Attempting to reconnect...")
            self.reconnect()
        if not connection:
            self.logger_database_ops.error("Reconnection failed. Cannot execute query.")
            return None
        try:
            with connection.cursor() as cursor:
                cursor.execute(query, params or {})
                results = cursor.fetchall()
                self.logger_database_ops.info(f"Query executed successfully: {query}")
                return results
        except Exception as e:
            self.logger_database_ops.error(f"Error executing query: {str(e)}")
            return None

    def execute_non_query(self, connection, query, params=None):
        """Execute INSERT, UPDATE, DELETE, or DDL queries."""
        if not connection:
            self.logger_database_ops.warning("No active database connection. Attempting to reconnect...")
            self.reconnect()
        if not connection:
            self.logger_database_ops.error("Reconnection failed. Cannot execute query.")
            return None
        try:
            with connection.cursor() as cursor:
                cursor.execute(query, params or {})
                connection.commit()
                self.logger_database_ops.info(f"Non-query executed successfully: {query}")
        except Exception as e:
            self.logger_database_ops.error(f"Error executing non-query: {str(e)}")
            connection.rollback()
    
    def create_table(self, connection, query):
        return self.execute_non_query(connection, query)

    def get_data(self, connection, query, params=None):
        """
        Fetch data using a SELECT query.
        """
        return self.execute_query(connection, query, params)

    def update_table(self, connection, table_name, set_clause, where_clause=None, params=None):
        """
        Update data in a table.
        """
        query = f"UPDATE {table_name} SET {set_clause}"
        if where_clause:
            query += f" WHERE {where_clause}"
        self.execute_non_query(connection, query, params)

    def delete_table_data(self, connection, table_name, where_clause=None, params=None):
        """
        Delete data from a table with optional WHERE clause.
        """
        query = f"DELETE FROM {table_name}"
        if where_clause:
            query += f" WHERE {where_clause}"
        self.execute_non_query(connection, query, params)

    def drop_table(self, connection, table_name, params=None):
        """
        Drop a table from the database.
        """
        query = f"DROP TABLE {table_name}"
        self.execute_non_query(connection, query, params)

    def rename_table(self, connection, old_table_name, new_table_name):
        """
        Rename an existing table.
        """
        query = f"ALTER TABLE {old_table_name} RENAME TO {new_table_name}"
        self.execute_non_query(connection, query)

    def alter_column_type(self, connection, table_name, column_name, new_data_type):
        """
        Alter the data type of a column in a table.
        """
        query = f"ALTER TABLE {table_name} MODIFY ({column_name} {new_data_type})"
        self.execute_non_query(connection, query)

    def rename_column(self, connection, table_name, old_column_name, new_column_name):
        """
        Rename a column in a table.
        """
        query = f"ALTER TABLE {table_name} RENAME COLUMN {old_column_name} TO {new_column_name}"
        self.execute_non_query(connection, query)

    def insert_data_from_dataframe(self, connection, table_name, dataframe):
        """
        Insert data from a Pandas DataFrame into an Oracle table.

        Args:
            connection: Active database connection.
            table_name (str): The target table name.
            dataframe (pd.DataFrame): The DataFrame containing data to insert.
        """
        try:
            if dataframe.empty:
                self.logger_database_ops.warning(f"No data to insert into '{table_name}'. DataFrame is empty.")
                return

            # Chuyển đổi cột Update_time về định dạng phù hợp
            if 'UPDATE_TIME' in dataframe.columns:
                dataframe['UPDATE_TIME'] = pd.to_datetime(dataframe['UPDATE_TIME'], errors='coerce')
            columns = dataframe.columns.tolist()
            placeholders = ", ".join([f":{col}" for col in columns])
            insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
            data_to_insert = dataframe.to_dict(orient="records")

            with connection.cursor() as cursor:
                cursor.executemany(insert_query, data_to_insert)
                connection.commit()

            self.logger_database_ops.info(f"Successfully inserted {len(data_to_insert)} rows into '{table_name}'.")

        except Exception as e:
            self.logger_database_ops.error(f"Error inserting data into '{table_name}': {str(e)}")
            connection.rollback()
    

