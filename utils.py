import os
import sys
from datetime import datetime
from typing import Union, Tuple, List
sys.path.append(os.path.dirname(os.getcwd()))
from model import LineageMantaObject, LineageMantaObjectOptimize
import pandas as pd
import numpy as np
from config import IMPORT_MANTA_FILE_PATH, LINEAGE_MANTA_FILE_INFO, PYTHON_TO_ORACLE_TYPE_MAPPING
from logger_module import setup_logger_global
logger_utils = setup_logger_global(os.path.basename(__file__), os.path.basename(__file__) + '.log')

def create_raw_table(
    object: Union[LineageMantaObject, LineageMantaObjectOptimize],
) -> Union[Tuple[pd.DataFrame, List[str]], None]:
    """Create a raw table from the given data."""
    try:
        df_raw = None
        table_name_list = []

        for key, value in LINEAGE_MANTA_FILE_INFO.items():
            if key[0] == "FALSE":
                continue
            
            source_name = key[1]
            print(source_name)

            if len(value) == 1:
                table_name, file_name = list(value.items())[0]
                print_table_info(table_name, file_name)
                table_name_list.append(table_name)
                df_raw = read_and_process_data(object, file_name, table_name)
                break 
            for table_name, file_name in value.items():
                print_table_info(table_name, file_name)
                df_name = read_and_process_data(object, file_name, table_name)
                table_name_list.append(table_name)
                # Kết hợp dữ liệu
                df_name = df_name.loc[:, ~df_name.columns.str.contains("^Unnamed")]
                df_raw = df_name if df_raw is None else object.concat_multi_dataframe(
                    dataframes=[df_raw, df_name], concat_type="vertical"
                )

        # Thêm cột thời gian cập nhật
        if df_raw is not None:
            df_raw["UPDATE_TIME"] = pd.to_datetime(datetime.now()).tz_localize("Asia/Bangkok").strftime('%Y-%m-%d %H:%M:%S')
        df_raw = df_raw.loc[:, ~df_raw.columns.str.contains("^Unnamed")]
        return df_raw, table_name_list

    except Exception as e:
        logger_utils.error(f"Error creating raw table: {e}")
        return None

def print_table_info(table_name: str, file_name: str):
    """In thông tin bảng và tệp."""
    print(
        "TABLE_NAME = ",
        table_name,
        "| FILE_NAME= ",
        file_name,
        "| file_path = ",
        f"{IMPORT_MANTA_FILE_PATH}/{file_name}.csv",
    )

def read_and_process_data(object: Union[LineageMantaObject, LineageMantaObjectOptimize], file_name: str, table_name: str) -> pd.DataFrame:
    """Đọc dữ liệu từ tệp và thêm tên bảng vào DataFrame."""
    df = object.read_data_from_file(
        file_path=f"{IMPORT_MANTA_FILE_PATH}/{file_name}.csv"
    )
    object.add_name_of_table_column(df=df, table_name=table_name)
    return df

def convert_df_to_oracle_format(df: pd.DataFrame) -> pd.DataFrame:
    """
    Chuyển đổi kiểu dữ liệu của DataFrame theo mapping PYTHON_TO_ORACLE_TYPE_MAPPING.
    
    Args:
        df (pd.DataFrame): DataFrame cần chuyển đổi.
    
    Returns:
        pd.DataFrame: DataFrame đã chuẩn hóa kiểu dữ liệu theo Oracle.
    """
    for column in df.columns:
        dtype = str(df[column].dtype)

        if dtype in PYTHON_TO_ORACLE_TYPE_MAPPING:
            oracle_type = PYTHON_TO_ORACLE_TYPE_MAPPING[dtype]

            # Xử lý kiểu dữ liệu theo mapping
            if "VARCHAR2" in oracle_type:
                df[column] = df[column].astype(str)  # Vectorized

            elif "TIMESTAMP" in oracle_type:
                df[column] = pd.to_datetime(df[column], errors='coerce')  # Vectorized

            elif "NUMBER" in oracle_type:
                if "int" in dtype:
                    df[column] = df[column].astype("int32")  # Vectorized
                elif "float" in dtype:
                    df[column] = df[column].astype("float32")  # Vectorized

            elif "CHAR(1)" in oracle_type and dtype == "bool":
                df[column] = df[column].map({True: "Y", False: "N"})  # Nhanh hơn apply()

    # Thay NaN bằng None để tránh lỗi khi insert vào Oracle
    return df.where(pd.notnull(df), None)


def transfrom_column_name_before_create_table(df):
    """
    Change column names before creating table
    
    Parameters:
    - df (pd.DataFrame): Input DataFrame
    """
    try:

        column_name_dict = {
            "Root_ID": "ROOT_ID",
            "Step_Node": "STEP_NODE",
            "Flow": "FLOW",
            "Node": "NODE",
            "Flow_implement_raw": "FLOW_RAW",
            "Raw_Node": "NODE_OBJECT",
            'table_name_extract': 'TABLE_NAME_NODE_OBJECT',
            'ValueObjectType': 'VALUE_OBJECT_TYPE',
            'database_type': 'SOURCE_DATABASE_TYPE',
            'database_name': 'SOURCE_DATABASE_NAME',
            'schema': 'SCHEMA_OBJECT',
            'source_table_name': 'SOURCE_TABLE_NAME',
            'column_name_relations': 'COLUMN_NAME_RELATIONS',
            'table_name_relations':'TABLE_NAME_RELATIONS',
            'unpivoted_column': 'NODE_RELATIONS',
            'database_type_relations': 'DATABASE_RELATIONS_NODE_TYPE',
            'database_name_relations': 'DATABASE_RELATIONS_NODE_NAME',
            'schema_relations': 'SCHEMA_RELATIONS_NODE',


            "Table_name": "TABLE_NAME_CSV",
            "Type": "TYPE",
            "column_name":"COLUMN_NAME_OBJECT",
            "SourcePath": "SOURCE_PATH",
            "TargetPath": "TARGET_PATH",
            "SourceColumnName": "SOURCE_COLUMN_NAME",
            "SourceColumnType": "SOURCE_COLUMN_TYPE",
            "TargetColumnName": "TARGET_COLUMN_NAME",
            "TargetColumnType": "TARGET_COLUMN_TYPE",
            "SourceObjectName": "SOURCE_OBJECT_NAME",
            "SourceObjectType": "SOURCE_OBJECT_TYPE",
            "TargetObjectName": "TARGET_OBJECT_NAME",
            "TargetObjectType": "TARGET_OBJECT_TYPE",
            "SourceGroupName": "SOURCE_GROUP_NAME",
            "SourceGroupType": "SOURCE_GROUP_TYPE",
            "TargetGroupName": "TARGET_GROUP_NAME",
            "TargetGroupType": "TARGET_GROUP_TYPE",
            "SourceResourceName": "SOURCE_RESOURCE_NAME",
            "SourceResourceType": "SOURCE_RESOURCE_TYPE",
            "TargetResourceName": "TARGET_RESOURCE_NAME",
            "TargetResourceType": "TARGET_RESOURCE_TYPE",
            "RevisionState": "REVISION_STATE",
            "UpdateTime": "UPDATE_TIME"


        }
        
        # Đổi tên các cột trong DataFrame
        df.rename(columns=column_name_dict, inplace=True)
        if "UPDATE_TIME" not in df.columns:
            df["UPDATE_TIME"] = pd.to_datetime(datetime.now())
        return df
    except Exception as e:
        logger_utils.error(f"Error transforming column name: {e}")
        return None