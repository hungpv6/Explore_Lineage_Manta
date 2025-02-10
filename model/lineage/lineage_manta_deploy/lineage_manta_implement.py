import os
import sys
from pathlib import Path
sys.path.append(str(Path(os.path.abspath(__file__)).parents[3]))
from config import LINEAGE_SOURCE, LINEAGE_TARGET
from logger_module import setup_logger_global
from model.lineage.lineage_exploit_data import LineageManta, LineageMantaOptimize
from model.lineage.utils import PreProcessingData
import pandas as pd
import datetime

class LineageMantaObject(LineageManta):
    def __init__(self,
                 lineage_name='',
                 **kwargs):
        """
        Khởi tạo đối tượng LineageMantaObject với việc kế thừa từ 3 lớp
        
        Parameters:
        - table_name: Tên bảng
        - database_name: Tên cơ sở dữ liệu
        - lineage_name: Tên lineage
        """
        # Gọi phương thức khởi tạo của các lớp cha
        super().__init__()
        
        # Các thuộc tính riêng của lớp
        self.lineage_name = lineage_name
        
        # Thiết lập logger
        connection_logger_name = os.path.basename(__file__)
        self.lineage_manta_object_logger = setup_logger_global(
            connection_logger_name, 
            connection_logger_name + '.log'
        )
        
        # Xử lý các tham số bổ sung
        for key, value in kwargs.items():
            setattr(self, key, value)



    def deploy_lineage_manta(self, df_raw, combo_list_copy,
                             source_col, target_col,
                             dictionary, 
                             adjacency_list,
                             columns_name = ['Root_ID', 'Step_Node', 'Flow',  'Node'], 
                             flow_name='Flow',
                             id_vars = ['Root_ID', 'Step_Node', 'Flow',  'Node', 'Flow_implement_raw'],
                             node_name='Node',
                             flow_implement_raw = 'Flow_implement_raw',
                             value_vars = [LINEAGE_SOURCE, LINEAGE_TARGET],
                             Raw_Node_column_name = 'Raw_Node',
                             character_split = ',',
                             remove_duplicate_columns = ['Flow_implement_raw', 'Raw_Node'],
                             select_columns = ['Flow_implement_raw', 'Raw_Node'],
                             filter_object_type_list = ['Table','View','PLSQL'],
                             value_object_type_name = 'ValueObjectType',
                             sort_by = None):
        """
        Khởi tạo biểu đồ dòng chảy LineageManta, sau đó transform biểu đồ thành
        dataframe và filter các bảng, view, PLSQL dựa trên các tham số đầu vào.
        
        Parameters:
        df_raw (pandas.DataFrame): Biểu đồ dòng chảy dạng bảng.
        combo_list_copy (list): Danh sách các node đã được loại bỏ.
        source_col (str): Tên cột của node nguồn.
        target_col (str): Tên cột của node đích.
        dictionary (dict): Từ điển các node và các giá trị của chúng.
        adjacency_list (list): Danh sách các cạnh của biểu đồ.
        columns_name (list, optional): Danh sách các cột của biểu đồ. Defaults to ['Root_ID', 'Step_Node', 'Flow',  'Node'].
        flow_name (str, optional): Tên cột của flow. Defaults to 'Flow'.
        id_vars (list, optional): Danh sách các cột giữ nguyên. Defaults to ['Root_ID', 'Step_Node', 'Flow',  'Node', 'Flow_implement_raw'].
        node_name (str, optional): Tên cột của node. Defaults to 'Node'.
        flow_implement_raw (str, optional): Tên cột của flow implement raw. Defaults to 'Flow_implement_raw'.
        value_vars (list, optional): Danh sách các cột giá trị. Defaults to [LINEAGE_SOURCE, LINEAGE_TARGET].
        Raw_Node_column_name (str, optional): Tên cột của raw node. Defaults to 'Raw_Node'.
        character_split (str, optional): Ký tự tách các giá trị. Defaults to ','.
        remove_duplicate_columns (list, optional): Danh sách các cột cần loại bỏ các giá trị trùng. Defaults to ['Flow_implement_raw', 'Raw_Node'].
        select_columns (list, optional): Danh sách các cột cần chọn. Defaults to ['Flow_implement_raw', 'Raw_Node'].
        filter_object_type_list (list, optional): Danh sách các đối tượng cần lọc. Defaults to ['Table','View','PLSQL'].
        sort_by (list, optional): Danh sách các cột cần sắp xếp. Defaults to None.
        value_object_type_name (str, optional): Tên cót giá trị của đối tượng. Defaults to 'ValueObjectType'.
        Returns:
        pandas.DataFrame: Biểu đồ dòng chảy sau khi filter và transform.
        """
        try:
           
            graph = self.process_lineage(combo_list_copy = combo_list_copy, 
                                         dictionary = dictionary, 
                                         adjacency_list = adjacency_list)
            
            df_graph = self.create_dataframe_from_graph(graph = graph, 
                                                        columns_name = columns_name, 
                                                        character_split = character_split,
                                                        sort_by = sort_by)
            
            df_update_dataframe = self.update_dataframe_with_dictionary(df = df_graph, 
                                                                        dictionary = dictionary, 
                                                                        character_split = character_split, 
                                                                        node_name = node_name, 
                                                                        flow_name = flow_name, 
                                                                        flow_implement_raw = flow_implement_raw)
            
            df_transformed = self.transform_dataframe(df = df_update_dataframe, 
                                                      id_vars = id_vars, 
                                                      value_vars = value_vars, 
                                                      Raw_Node_column_name = Raw_Node_column_name)
            if remove_duplicate_columns is not None:
                df_transformed.drop_duplicates(subset = remove_duplicate_columns).reset_index(drop=True)

            if select_columns is not None:
                df_transformed = df_transformed[select_columns]

           
            object_node_dict = self.filter_object_dict(df=df_raw, source_col=source_col, target_col=target_col)
            table_filter_ = self.filter_table_from_object_type(table_filter = df_transformed, 
                                                               column_filter=Raw_Node_column_name, 
                                                               filter_object_type_list=filter_object_type_list, object_node_dict=object_node_dict, value_object_type_name = value_object_type_name)

        
            return table_filter_
        
        except Exception as e:
            self.lineage_manta_object_logger.error(f"Error in deploy_lineage_manta: {e}")
            return []

    

    # def final_table(self, df_transform):
    #     df_final = df_transform.copy()
        # combo_list_copy = [1,2,1868,72,824,928,40,121,366,1691,35,548,663
    # 		,1944,509,638,1800,620,750,54,1509,2166,1531,1173,1013,74,345,245
    # 		,762,2165,1439,1525,75,631,840,1734,1738,2001,728,1728,1987,1341,145,1724,22,282]
    
class LineageMantaObjectOptimize(LineageMantaOptimize):
    def __init__(self, lineage_name=''):
        super().__init__()
        self.lineage_name = lineage_name
        self.logger_lineage_manta_optimize = setup_logger_global(os.path.basename(__file__), os.path.basename(__file__) + '.log')


    def deploy_lineage_manta_optimized(self, df_raw, combo_list_copy,
                             source_col, target_col,
                             dictionary, 
                             adjacency_list,
                             columns_name = ['Root_ID', 'Step_Node', 'Flow',  'Node'], 
                             flow_name='Flow',
                             id_vars = ['Root_ID', 'Step_Node', 'Flow',  'Node', 'Flow_implement_raw'],
                             node_name='Node',
                             flow_implement_raw = 'Flow_implement_raw',
                             value_vars = [LINEAGE_SOURCE, LINEAGE_TARGET],
                             Raw_Node_column_name = 'Raw_Node',
                             character_split = ',',
                             remove_duplicate_columns = ['Flow_implement_raw', 'Raw_Node'],
                             select_columns = ['Flow_implement_raw', 'Raw_Node'],
                             filter_object_type_list = ['Table','View','PLSQL'],
                             value_object_type_name = 'ValueObjectType',
                             sort_by = None):
        """
        Khởi tạo biểu đồ dòng chảy LineageManta, sau đó transform biểu đồ thành
        dataframe và filter các bảng, view, PLSQL dựa trên các tham số đầu vào.
        
        Parameters:
        df_raw (pandas.DataFrame): Biểu đồ dòng chảy dạng bảng.
        combo_list_copy (list): Danh sách các node đã được loại bỏ.
        source_col (str): Tên cột của node nguồn.
        target_col (str): Tên cột của node đích.
        dictionary (dict): Từ điển các node và các giá trị của chúng.
        adjacency_list (list): Danh sách các cạnh của biểu đồ.
        columns_name (list, optional): Danh sách các cột của biểu đồ. Defaults to ['Root_ID', 'Step_Node', 'Flow',  'Node'].
        flow_name (str, optional): Tên cột của flow. Defaults to 'Flow'.
        id_vars (list, optional): Danh sách các cột giữ nguyên. Defaults to ['Root_ID', 'Step_Node', 'Flow',  'Node', 'Flow_implement_raw'].
        node_name (str, optional): Tên cột của node. Defaults to 'Node'.
        flow_implement_raw (str, optional): Tên cột của flow implement raw. Defaults to 'Flow_implement_raw'.
        value_vars (list, optional): Danh sách các cột giá trị. Defaults to [LINEAGE_SOURCE, LINEAGE_TARGET].
        Raw_Node_column_name (str, optional): Tên cột của raw node. Defaults to 'Raw_Node'.
        character_split (str, optional): Ký tự tách các giá trị. Defaults to ','.
        remove_duplicate_columns (list, optional): Danh sách các cột cần loại bỏ các giá trị trùng. Defaults to ['Flow_implement_raw', 'Raw_Node'].
        select_columns (list, optional): Danh sách các cột cần chọn. Defaults to ['Flow_implement_raw', 'Raw_Node'].
        filter_object_type_list (list, optional): Danh sách các đối tượng cần lọc. Defaults to ['Table','View','PLSQL'].
        sort_by (list, optional): Danh sách các cột cần sắp xếp. Defaults to None.
        value_object_type_name (str, optional): Tên cót giá trị của đối tượng. Defaults to 'ValueObjectType'.
        Returns:
        pandas.DataFrame: Biểu đồ dòng chảy sau khi filter và transform.
        """
        try:
           
            graph = self.process_lineage(combo_list_copy = combo_list_copy, 
                                         dictionary = dictionary, 
                                         adjacency_list = adjacency_list)
            return graph
        except Exception as e:
            self.logger_lineage_manta_optimize.error(f"Error in deploy_lineage_manta: {e}")
            return []
