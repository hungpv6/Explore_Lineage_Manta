import os
import sys
from pathlib import Path
import time
sys.path.append(str(Path(os.path.abspath(__file__)).parents[3]))
import pandas as pd
from model.optimize_algo import Trie
from logger_module import setup_logger_global

class PreProcessingData:
    def __init__(self):
        # self.db = Database(**db_config)
        """
        Initialize the PreProcessingData class.

        Sets up the logger for the data service.

        :param db_config: A dictionary with the database configuration
        :type db_config: dict
        """
        connection_logger_name = os.path.basename(__file__)
        self.logger_data_service = setup_logger_global(connection_logger_name, connection_logger_name + '.log')
        

    def read_data_from_file(self, file_path):
        """
        Reads data from a CSV file and returns it as a Pandas DataFrame.
        
        If there is an error reading the file, it logs the error and returns None.
        
        :param file_path: The path to the CSV file to read
        :type file_path: str
        :return: The Pandas DataFrame containing the data or None if there was an error
        :rtype: pd.DataFrame or None
        """
        try:
            df = pd.read_csv(file_path)
            return df
        except Exception as e:
            self.logger_data_service.error(f"Error reading data from file: {e}")
            return None
    
    
    def add_name_of_table_column(self, df, table_name, column_table_name = 'Table_name'):
        """
        Adds a column to the DataFrame with the name of the table.
        
        :param df: The DataFrame to add the column to
        :type df: pd.DataFrame
        :param table_name: The name of the table
        :type table_name: str
        :param column_table_name: The name of the column to add, defaults to 'Table_name'
        :type column_table_name: str, optional
        :return: The DataFrame with the added column
        :rtype: pd.DataFrame
        """
        try:
            df[column_table_name] = table_name
            return df
        except Exception as e:
            self.logger_data_service.error(f"Error adding name of table column: {e}")
            return df
    
    def concat_multi_dataframe(self, dataframes, concat_type = 'vertical', ignore_index=True):
        """
        Concatenates multiple DataFrames into one.

        This function takes a list of DataFrames and concatenates them into one, either vertically or horizontally.

        Parameters
        ----------
        dataframes : list of pd.DataFrame
            The list of DataFrames to concatenate
        concat_type : str, optional
            The type of concatenation to use. Either 'vertical' or 'horizontal'. Defaults to 'vertical'.
        ignore_index : bool, optional
            Whether to ignore the index when concatenating the DataFrames. Defaults to True.

        Returns
        -------
        pd.DataFrame
            The concatenated DataFrame, or None if there was an error.
        """
        try:
            if concat_type == 'vertical':
                df_concat = pd.concat(dataframes, axis = 0, ignore_index=ignore_index)
            elif concat_type == 'horizontal':
                df_concat = pd.concat(dataframes, axis = 1, ignore_index=ignore_index)
            return df_concat
        except Exception as e:
            self.logger_data_service.error(f"Error reading data from file: {e}")
            return None
    
    def transform_list_to_dataframe(self, list_value, column_name, table_input=None):
        """
        Transforms a list to a Pandas DataFrame.

        Parameters
        ----------
        list_value : list
            The list to transform.
        column_name : str or list
            The column name for the DataFrame.
        table_input : pd.DataFrame, optional
            The DataFrame to add the list to. If None, a new DataFrame is created.

        Returns
        -------
        pd.DataFrame
            The DataFrame with the list transformed.
        """
        if not isinstance(list_value, list):
            self.logger_data_service.error("Error: List_value is not a list.")
        if not isinstance(column_name, (str, list)):
            self.logger_data_service.error("Error: Column_name is not a string or list.")
        
        if table_input is None:
            if isinstance(column_name, str):
                column_name = [column_name]
            if len(column_name) != 1:
                self.logger_data_service.error("Error: Column_name must be a string or a list of length 1.")
            table_input = pd.DataFrame(list_value, columns=column_name)
        else:
            if not isinstance(table_input, pd.DataFrame):
                self.logger_data_service.error("Error: Table_input is not a DataFrame.")
            table_input[column_name] = list_value        
        return table_input
    
    def transform_dataframe_to_list(self, df_input, column_name):
        """
        Transforms a DataFrame column to a list.

        Parameters
        ----------
        df_input : pd.DataFrame
            The DataFrame to transform.
        column_name : str
            The column name to transform.

        Returns
        -------
        list
            The list transformed from the DataFrame column.
        """
        if not isinstance(df_input, pd.DataFrame):
            self.logger_data_service.error("Error: Df_input is not a DataFrame.")
        if not isinstance(column_name, str):
            self.logger_data_service.error("Error: Column_name is not a string.")
        list_node = df_input[column_name].tolist()
        return list_node
    
    def export_to_csv(self, df: pd.DataFrame, file_path: str, index: bool = False) -> None:
        """
        Exports a Pandas DataFrame to a CSV file.

        Parameters
        ----------
        df : pd.DataFrame
            The DataFrame to export.
        file_path : str
            The path to the CSV file to write.
        index : bool, optional
            Whether to write row names (indices). Defaults to False.

        Logs
        ----
        Logs a success message if the export is successful, or an error message if an exception occurs.
        """

        try:
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            df.to_csv(file_path, index=index)
            self.logger_data_service.info(f"Data successfully exported to {file_path}")
        
        except Exception as e:
            self.logger_data_service.error(f"Error exporting data to CSV: {e}")
    
    def remove_subsets_trie(self, paths):
        """
        Loại bỏ các path là tập con của path khác sử dụng Trie để tối ưu.
        """
        try:
            paths = sorted(paths, key=len)  # Đảm bảo đường đi ngắn hơn được xét trước
            trie = Trie()
            filtered_paths = []

            for path in paths:
                self.logger_data_service.info(f"Processing path: {path}")
                if not trie.is_prefix(path):  # Nếu chưa có trong Trie thì giữ lại
                    filtered_paths.append(path)
                    trie.insert(path)  # Thêm vào Trie để kiểm tra nhanh hơn
                    self.logger_data_service.info(f"Added path to Trie: Trie {trie}")
                    self.logger_data_service.info(f"Filtered paths: {filtered_paths}")

            return filtered_paths
        except Exception as e:
            self.logger_data_service.error(f"Error removing subsets: {e}")

    def estimate_table_size(self, file_path=None, df=None, table_sql=None, connection=None):
        """
        Đọc file hoặc bảng SQL để ước lượng kích thước bảng hash.
        - file_path: Đường dẫn file CSV hoặc SQL query/table
        - chunk_size: Số dòng đọc mỗi lần (giúp xử lý file lớn)

        Returns:
        - int: Kích thước bảng hash (gấp đôi số dòng để tránh collision)
        """
        
        if file_path is not None and file_path.endswith('.csv'):
            row_count = sum(1 for _ in open(file_path)) - 1  # Bỏ dòng header
        elif df is not None:
            row_count = len(df)
        elif table_sql is not None and connection is not None:
            df = pd.read_sql(f"SELECT COUNT(*) FROM {table_sql}", connection)
            row_count = df.iloc[0, 0]
        else:
            return None   
        return 2 * row_count
    
    def transform_list_to_dict(self,list_transform):
        """
        Transforms a list of lists into a dictionary where the keys are the indices of the sublists and the values are lists of the elements at that index.

        Parameters
        ----------
        list_transform : list of lists
            A list containing sublists from which to create a dictionary.

        Returns
        -------
        dict
            A dictionary where the keys are the indices of the sublists and the values are lists of the elements at that index.
        """
        result_dict = {}
        for i  in  range(len(list_transform)):
            if i not in list(result_dict.keys()):
                result_dict[i] = []
            result_dict[i].append(list_transform[i])
        return result_dict
    
    def unpivoted_column(self, table, column_unpivoted, change_column_data_type = int):
        """
        Unpivots a specified column in a DataFrame, converting it into separate rows.

        Parameters
        ----------
        table : pd.DataFrame
            The input DataFrame containing the column to be unpivoted.
        column_unpivoted : str
            The name of the column in the DataFrame to be unpivoted.
        change_column_data_type : type, optional
            The data type to convert the unpivoted column values to, by default int.

        Returns
        -------
        pd.DataFrame
            A DataFrame with the specified column unpivoted and each value 
            in the column converted to the specified data type.

        Raises
        ------
        Exception
            If an error occurs during the unpivoting or data type conversion process.
        """

        try:
            df_unpivoted = table.explode(column_unpivoted).reset_index(drop=True)
            df_unpivoted[column_unpivoted] = df_unpivoted[column_unpivoted].astype(change_column_data_type)
            return df_unpivoted
        except Exception as e:
            self.logger_data_service.error(f"Error: {e}")
    

    #  transform data from db to dataframe
        

    # def __del__(self):
    #     self.db.disconnect()