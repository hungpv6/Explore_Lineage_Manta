import cx_Oracle
import json
import os
import sys
from pathlib import Path
sys.path.append(str(Path(os.path.abspath(__file__)).parents[3]))
from logger_module import setup_logger_global
from config import CONNECTION_CONFIG


class RepoConnection:
    def __init__(self):
        """
        Initialize database connection details.
        
        Parameters:
            db_name (str): Database name.
            user (str): Database username.
            password (str): Database password.
            host (str): Database host address.
            port (int): Database port number.
            service_name (str): Database service name.
            environment (str): Environment (e.g., 'DEV', 'PROD'). Default is 'DEV'.
        """
        connection_logger_name = os.path.basename(__file__)
        self.database_connect_error_logger = setup_logger_global(connection_logger_name, connection_logger_name + '.log')

    def load_config(self):
        try:
            with open(CONNECTION_CONFIG, "r") as f:
                config = json.load(f)
            return config
        except Exception as e:
            self.database_connect_error_logger.error(f"Error loading configuration: {e}")

    def get_connection(self, db_name, environment, user):
        """
        Establish and return a database connection.

        Parameters:
            db_name (str): Database name. Defaults to instance-level db_name.
            user (str): Database username. Defaults to instance-level user.
            password (str): Database password. Defaults to instance-level password.
            host (str): Database host address. Defaults to instance-level host.
            port (int): Database port number. Defaults to instance-level port.
            service_name (str): Database service name. Defaults to instance-level service_name.
            environment (str): Environment (e.g., 'DEV', 'PROD'). Defaults to instance-level environment.

        Returns:
            connection (cx_Oracle.Connection): Oracle database connection object.
            None: If connection fails.
        """
        # Use instance-level defaults if parameters are not provided
        try:
            # Load configuration from JSON file
            
            config = self.load_config()
            # Retrieve connection info based on environment and database name
            connection_info = config[environment][db_name][user]
            password = connection_info["password"]
            host = connection_info["host"]
            port = connection_info["port"]
            service_name = connection_info["service_name"]

            # Create DSN (Data Source Name) and establish connection
            dsn = cx_Oracle.makedsn(host, port, service_name=service_name)
            connection = cx_Oracle.connect(user=user, password=password, dsn=dsn)

            print(f"Connection to database '{db_name}' established successfully.")
            return connection
        except Exception as e:
            self.database_connect_error_logger.error(f"Unexpected error: {e} - Cannot connect to database {db_name} - user {user} - host {host} - port {port} - service_name {service_name} .")
            return None
        
