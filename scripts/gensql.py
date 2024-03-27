# 定义变量
import datetime
import os

DATAHOUSE_DIR = "s3a://nineinfra/datahouse"
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


def generate_etl2ods_full_sql(start_date):
    SQL_LOAD_LOG = f"load data inpath '{DATAHOUSE_DIR}{ETL_LOG_DIR}/{start_date}' into table {DATA_BASE}.ods_log_inc partition(dt='{start_date}');\n"
    SQL_FILE_DATA = SQL_LOAD_LOG
    # 从full目录下加载该表的所有文件,文件格式为 2024.03.25_0.parquet
    # 如果表数据量较小，一天同步完成，则可能包含同日期的多个分区文件，如,2024.03.26_0.parquet,,2024.03.26_1.parquet
    # 如果表数据量很大，一天同步完成不了，则可能包含多个日期的多个分区文件，如,2024.03.25_0.parquet,,2024.03.26_1.parquet
    for table in FULL_LOAD_TABLES:
        ods_table = f'ods_{table}_full'
        SQL_FILE_DATA += f"load data inpath '{DATAHOUSE_DIR}{ETL_DB_FULL_DIR}/{table}' overwrite into table {DATA_BASE}.{ods_table} partition(dt='{start_date}');\n"

    # 将 SQL_FILE_DATA 写入文件
    with open(f"{SQL_FILE_DIR}/etl2ods_full.sql", 'w') as f:
        f.write(SQL_FILE_DATA)


def generate_etl2ods_inc_sql(start_date):
    SQL_LOAD_LOG = f"load data inpath '{DATAHOUSE_DIR}{ETL_LOG_DIR}/{start_date}' into table {DATA_BASE}.ods_log_inc partition(dt='{start_date}');\n"
    SQL_FILE_DATA = SQL_LOAD_LOG

    # 从Inc目录下加载该表的所有日期为start_date的文件,文件格式为 2024.03.25_0.parquet，2024.03.25_1.parquet等等
    for table in INC_LOAD_TABLES:
        ods_table = f'ods_{table}_full'
        # 获取{ETL_DB_INC_DIR}/{table}这个目录下以{start_date}为前缀的文件列表，使用通配符*实现
        SQL_FILE_DATA += f"load data inpath '{DATAHOUSE_DIR}{ETL_DB_INC_DIR}/{table}/{start_date}*' into table {DATA_BASE}.{ods_table} partition(dt='{start_date}');\n"

    # 将 SQL_FILE_DATA 写入文件
    with open(f"{SQL_FILE_DIR}/etl2ods_inc.sql", 'w') as f:
        f.write(SQL_FILE_DATA)


if __name__ == '__main__':
    generate_etl2ods_full_sql(datetime.date(2024, 3, 25))
