import os
import sys
from pathlib import Path
print('os.getcwd(),', os.getcwd())
# sys.path.append(str(Path(os.path.abspath(__file__)).parents[1]))
# print(sys.path.append(str(Path(os.path.abspath(__file__)).parents[1])))
from logger_module import setup_logger_global
from repo.repo_base import RepoTransformation


class RepoLineage(RepoTransformation):
    def __init__(self):
        RepoTransformation.__init__(self)
        self.logger_lineage_query = setup_logger_global(os.path.basename(__file__), os.path.basename(__file__) + '.log')

    
    def get_table_raw_data(self, connection, query):
        try:
            return self.get_data(
                connection = connection,
                query = query
            )
        except Exception as e:
            self.logger_lineage_query.error(f"Error in get_table_raw_data: {str(e)}")
    
    def drop_table_raw_data(self, connection, table_drop):
        try:
            return self.drop_table(
                connection = connection,
                table_name = table_drop)
        except Exception as e:
            self.logger_lineage_query.error(f"Error in drop_table_raw_data: {str(e)}")
    
    def create_lineage_manta_table(self, connection, table_name, column_definitions):
        """
        Create a table with the specified column definitions.
        """
        try:
            columns = []
            for column_name, properties in column_definitions.items():
                column_type = properties["type"]

                # Đổi kiểu `DATETIME` sang `TIMESTAMP`
                if column_type.upper() == "DATETIME":
                    column_type = "TIMESTAMP"

                columns.append(f'"{column_name}" {column_type}')
            
            columns_definition = ", ".join(columns)
            create_table_sql = f'CREATE TABLE "{table_name}" ({columns_definition})'
            
            return self.create_table(connection, create_table_sql)
        except Exception as e:
            self.logger_database_ops.error(f"Error in create_lineage_manta_table: {str(e)}")


    



    
