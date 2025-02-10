import sys
import os
from pathlib import Path
import pandas as pd
from .connection import RepoConnection
from .ops import RepoOps
from logger_module import setup_logger_global

class RepoTransformation(RepoOps, RepoConnection):
    def __init__(self):
        RepoOps.__init__(self)
        RepoConnection.__init__(self)
        self.logger_database_ops = setup_logger_global(os.path.basename(__file__), os.path.basename(__file__) + '.log')

    def get_table_as_dataframe(self, table_name, query, params_query = None):
        """Get a table from Oracle DB and return it as a Pandas DataFrame."""
        # Create a query to fetch all data from the table
        try:
            # Fetch the data using the get_data method
            results = self.get_data(query, params_query)
            
            if results is None:
                return None
            df = pd.DataFrame(results)
            self.logger_database_ops.info(f"Data fetched successfully from table '{table_name}' and converted to DataFrame.")
        except Exception as e:
            self.logger_database_ops.error(f"Error fetching data from table '{table_name}': {str(e)}")
        return df
