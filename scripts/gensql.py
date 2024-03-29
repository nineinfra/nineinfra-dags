import datetime
import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__)))
import ods2dwdinit
import ods2diminit
import ods2dim
import ods2dwd
import dwd2dwsinit
import dwd2dws
import dws2ads

DATA_BASE = "datahouse"
ETL_LOG_DIR = "/kafka/nineinfra"
ETL_DB_FULL_DIR = "/seatunnel/mysql/full"
ETL_DB_INC_DIR = "/seatunnel/mysql/inc"
SQL_FILE_DIR = os.path.join(os.path.dirname(__file__), "../sqls")

FULL_LOAD_TABLES = ["activity_info", "activity_rule", "base_category1",
                    "base_category2", "base_category3", "base_dic",
                    "base_province", "base_region", "base_trademark",
                    "cart_info", "coupon_info", "sku_attr_value",
                    "sku_info", "sku_sale_attr_value", "spu_info"]

INC_LOAD_TABLES = ["cart_info", "comment_info", "ods_coupon_use", "ods_favor_info",
                   "order_detail", "order_detail_activity", "ods_order_detail_coupon",
                   "order_info", "order_refund_info", "ods_order_status_log",
                   "payment_info", "refund_payment", "user_info"]


# 生成全量数据加载到ods的sql
def generate_etl2ods_full_sql(datahouse_dir, start_date):
    sql_load_log = f"load data inpath '{datahouse_dir}{ETL_LOG_DIR}/{start_date}' into table {DATA_BASE}.ods_log_inc partition(dt='{start_date}');\n"
    sql_file_data = sql_load_log
    # 从full目录下加载该表的所有文件,文件格式为 2024.03.25_0.parquet
    # 如果表数据量较小，一天同步完成，则可能包含同日期的多个分区文件，如,2024.03.26_0.parquet,,2024.03.26_1.parquet
    # 如果表数据量很大，一天同步完成不了，则可能包含多个日期的多个分区文件，如,2024.03.25_0.parquet,,2024.03.26_1.parquet
    for table in FULL_LOAD_TABLES:
        ods_table = f'ods_{table}_full'
        sql_file_data += f"load data inpath '{datahouse_dir}{ETL_DB_FULL_DIR}/{table}' overwrite into table {DATA_BASE}.{ods_table} partition(dt='{start_date}');\n"

    # 将 SQL_FILE_DATA 写入文件
    with open(f"{SQL_FILE_DIR}/etl2ods_full.sql", 'w') as f:
        f.write(sql_file_data)


# 生成增量数据加载到ods的sql
def generate_etl2ods_inc_sql(datahouse_dir, start_date):
    sql_load_log = f"load data inpath '{datahouse_dir}{ETL_LOG_DIR}/{start_date}' into table {DATA_BASE}.ods_log_inc partition(dt='{start_date}');\n"
    sql_file_data = sql_load_log

    # 从Inc目录下加载该表的所有日期为start_date的文件,文件格式为 2024.03.25_0.parquet，2024.03.25_1.parquet等等
    for table in INC_LOAD_TABLES:
        ods_table = f'ods_{table}_inc'
        # 获取{ETL_DB_INC_DIR}/{table}这个目录下以{start_date}为前缀的文件列表，使用通配符*实现
        sql_file_data += f"load data inpath '{datahouse_dir}{ETL_DB_INC_DIR}/{table}/{start_date}*' into table {DATA_BASE}.{ods_table} partition(dt='{start_date}');\n"

    # 将 SQL_FILE_DATA 写入文件
    with open(f"{SQL_FILE_DIR}/etl2ods_inc.sql", 'w') as f:
        f.write(sql_file_data)


# 生成初始化事实表的sql
def generate_ods2dwd_init_sql(start_date):
    dwd_init_sqls = ods2dwdinit.get_ods2dim_init_sqls(DATA_BASE, start_date)
    sql_file_data = ""
    for sql in dwd_init_sqls:
        sql_file_data += sql

    # 将 SQL_FILE_DATA 写入文件
    with open(f"{SQL_FILE_DIR}/ods2dwd_init.sql", 'w') as f:
        f.write(sql_file_data)


# 生成初始化维度表的sql
def generate_ods2dim_init_sql(datahouse_dir, start_date):
    dim_init_sqls = ods2diminit.get_ods2dim_init_sqls(datahouse_dir, DATA_BASE, start_date)
    sql_file_data = ""
    for sql in dim_init_sqls:
        sql_file_data += sql

    # 将 SQL_FILE_DATA 写入文件
    with open(f"{SQL_FILE_DIR}/ods2dim_init.sql", 'w') as f:
        f.write(sql_file_data)


# 生成维度表的sql
def generate_ods2dim_sql(start_date):
    dim_sqls = ods2dim.get_ods2dim_sqls(DATA_BASE, start_date)
    sql_file_data = ""
    for sql in dim_sqls:
        sql_file_data += sql

    # 将 SQL_FILE_DATA 写入文件
    with open(f"{SQL_FILE_DIR}/ods2dim.sql", 'w') as f:
        f.write(sql_file_data)


# 生成事实表的sql
def generate_ods2dwd_sql(start_date):
    dwd_sqls = ods2dwd.get_ods2dwd_sqls(DATA_BASE, start_date)
    sql_file_data = ""
    for sql in dwd_sqls:
        sql_file_data += sql

    # 将 SQL_FILE_DATA 写入文件
    with open(f"{SQL_FILE_DIR}/ods2dwd.sql", 'w') as f:
        f.write(sql_file_data)


# 生成初始化宽表的sql
def generate_dwd2dws_init_sql(start_date):
    dws_init_sqls = dwd2dwsinit.get_dwd2dws_init_sqls(DATA_BASE, start_date)
    sql_file_data = ""
    for sql in dws_init_sqls:
        sql_file_data += sql

    # 将 SQL_FILE_DATA 写入文件
    with open(f"{SQL_FILE_DIR}/dwd2dws_init.sql", 'w') as f:
        f.write(sql_file_data)


# 生成宽表的sql
def generate_dwd2dws_sql(start_date):
    dws_sqls = dwd2dws.get_dwd2dws_sqls(DATA_BASE, start_date)
    sql_file_data = ""
    for sql in dws_sqls:
        sql_file_data += sql

    # 将 SQL_FILE_DATA 写入文件
    with open(f"{SQL_FILE_DIR}/dwd2dws.sql", 'w') as f:
        f.write(sql_file_data)


# 生成应用表的sql
def generate_dws2ads_sql(start_date):
    ads_sqls = dws2ads.get_dws2ads_sqls(DATA_BASE, start_date)
    sql_file_data = ""
    for sql in ads_sqls:
        sql_file_data += sql

    # 将 SQL_FILE_DATA 写入文件
    with open(f"{SQL_FILE_DIR}/dws2ads.sql", 'w') as f:
        f.write(sql_file_data)


if __name__ == '__main__':
    MINIO_DATAHOUSE_DIR = "s3a://nineinfra/datahouse"
    generate_etl2ods_full_sql(MINIO_DATAHOUSE_DIR, datetime.date(2024, 3, 25))
    generate_etl2ods_inc_sql(MINIO_DATAHOUSE_DIR, datetime.date(2024, 3, 25))
    generate_ods2dwd_init_sql(datetime.date(2024, 3, 25))
    generate_ods2dim_init_sql(MINIO_DATAHOUSE_DIR, datetime.date(2024, 3, 25))
    generate_ods2dim_sql(datetime.date(2024, 3, 25))
    generate_ods2dwd_sql(datetime.date(2024, 3, 25))
    generate_dwd2dws_init_sql(datetime.date(2024, 3, 25))
    generate_dwd2dws_sql(datetime.date(2024, 3, 25))
    generate_dws2ads_sql(datetime.date(2024, 3, 25))
