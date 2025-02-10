from config import LINEAGE_MANTA_TABLE_TEST, LINEAGE_MANTA_TABLE_NAME_RAW


query_lineage_table = "SELECT * FROM lineage_table"
query_lineage_table_test =  f"SELECT * FROM {LINEAGE_MANTA_TABLE_TEST} WHERE ROW_NUM <= 1000"
query_lineage_table_name_raw = f"SELECT * FROM {LINEAGE_MANTA_TABLE_NAME_RAW} WHERE ROW_NUM <= 1000"