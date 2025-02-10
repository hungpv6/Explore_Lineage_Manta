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
            else:
                source_name = key[1]
                print(source_name)
                if len(value.keys()) == 1:
                    table_name = list(value.keys())[0]
                    file_name = list(value.values())[0]
                    print(
                        "TABLE_NAME = ",
                        table_name,
                        "| FILE_NAME= ",
                        file_name,
                        "| file_path = ",
                        f"{IMPORT_MANTA_FILE_PATH}/{file_name}.csv",
                    )
                    table_name_list.append(table_name)
                    df_raw = object.read_data_from_file(
                        file_path=f"{IMPORT_MANTA_FILE_PATH}/{file_name}.csv"
                    )
                    object.add_name_of_table_column(df=df_raw, table_name=table_name)
                    break
                else:
                    for table_name, file_name in value.items():
                        print(
                            "TABLE_NAME = ",
                            table_name,
                            "| FILE_NAME= ",
                            file_name,
                            "| file_path = ",
                            f"{IMPORT_MANTA_FILE_PATH}/{file_name}.csv",
                        )
                        df_name = object.read_data_from_file(
                            file_path=f"{IMPORT_MANTA_FILE_PATH}/{file_name}.csv"
                        )
                        table_name_list.append(table_name)
                        object.add_name_of_table_column(
                            df=df_name, table_name=table_name
                        )
                        if df_raw is None:
                            df_raw = df_name
                        else:
                            df_raw = object.concat_multi_dataframe(
                                dataframes=[df_raw, df_name], concat_type="vertical"
                            )
        df_raw["UPDATE_TIME"] = pd.to_datetime(datetime.now())
        return df_raw, table_name_list
    except Exception as e:
        logger_utils.error(f"Error creating raw table: {e}")
        return None

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

            # Chuyển về VARCHAR2 nếu là object (chuỗi)
            if "VARCHAR2" in oracle_type:
                df[column] = df[column].astype(str)

            # Chuyển datetime về format chuẩn
            elif "TIMESTAMP" in oracle_type:
                df[column] = pd.to_datetime(df[column], errors='coerce')

            # Chuyển số nguyên về int32
            elif "NUMBER" in oracle_type and "int" in dtype:
                df[column] = df[column].astype("int32")

            # Chuyển số thực về float32
            elif "NUMBER" in oracle_type and "float" in dtype:
                df[column] = df[column].astype("float32")

            # Chuyển bool thành CHAR(1) với giá trị 'Y' hoặc 'N'
            elif "CHAR(1)" in oracle_type and dtype == "bool":
                df[column] = df[column].apply(lambda x: "Y" if x else "N")

    # Thay NaN bằng None để tránh lỗi khi insert vào Oracle
    df = df.where(pd.notnull(df), None)

    return df

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
            "Raw_Node": "RAW_NODE",
            'ValueObjectType': 'VALUE_OBJECT_TYPE',
            'source_database_type': 'SOURCE_DATABASE_TYPE',
            'source_database_name': 'SOURCE_DATABASE_NAME',
            'schema': 'SCHEMA',
            'source_table_name': 'SOURCE_TABLE_NAME',
            'source_column_name': 'SOURCE_COLUMN_NAME',
            "Table_name": "TABLE_NAME",
            "Type": "TYPE",
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