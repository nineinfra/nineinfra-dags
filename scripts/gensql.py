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

DWD_INIT_SQLS = [ods2dwdinit.dwd_trade_cart_add_inc, ods2dwdinit.dwd_trade_order_detail_inc,
                 ods2dwdinit.dwd_trade_cancel_detail_inc, ods2dwdinit.dwd_trade_pay_detail_suc_inc,
                 ods2dwdinit.dwd_trade_order_refund_inc, ods2dwdinit.dwd_trade_refund_pay_suc_inc,
                 ods2dwdinit.dwd_trade_cart_full, ods2dwdinit.dwd_tool_coupon_get_inc,
                 ods2dwdinit.dwd_tool_coupon_order_inc, ods2dwdinit.dwd_tool_coupon_pay_inc,
                 ods2dwdinit.dwd_interaction_favor_add_inc, ods2dwdinit.dwd_interaction_comment_inc,
                 ods2dwdinit.dwd_traffic_page_view_inc, ods2dwdinit.dwd_traffic_start_inc,
                 ods2dwdinit.dwd_traffic_action_inc, ods2dwdinit.dwd_traffic_display_inc,
                 ods2dwdinit.dwd_traffic_error_inc, ods2dwdinit.dwd_user_register_inc, ods2dwdinit.dwd_user_login_inc]

DIM_INIT_SQLS = [ods2diminit.dim_date, ods2diminit.dim_user_zip, ods2diminit.dim_sku_full,
                 ods2diminit.dim_province_full, ods2diminit.dim_coupon_full, ods2diminit.dim_activity_full]

DIM_SQLS = [ods2dim.dim_user_zip, ods2dim.dim_sku_full, ods2dim.dim_province_full,
            ods2dim.dim_coupon_full, ods2dim.dim_activity_full]

DWD_SQLS = [ods2dwd.dwd_trade_cart_add_inc, ods2dwd.dwd_trade_order_detail_inc, ods2dwd.dwd_trade_cancel_detail_inc,
            ods2dwd.dwd_trade_pay_detail_suc_inc, ods2dwd.dwd_trade_order_refund_inc,
            ods2dwd.dwd_trade_refund_pay_suc_inc, ods2dwd.dwd_trade_cart_full, ods2dwd.dwd_tool_coupon_get_inc,
            ods2dwd.dwd_tool_coupon_order_inc, ods2dwd.dwd_tool_coupon_pay_inc, ods2dwd.dwd_interaction_favor_add_inc,
            ods2dwd.dwd_interaction_favor_add_inc, ods2dwd.dwd_traffic_page_view_inc, ods2dwd.dwd_traffic_start_inc,
            ods2dwd.dwd_traffic_action_inc, ods2dwd.dwd_traffic_display_inc, ods2dwd.dwd_traffic_error_inc,
            ods2dwd.dwd_user_register_inc, ods2dwd.dwd_user_login_inc]

DWS_INIT_SQLS = [dwd2dwsinit.dws_trade_user_sku_order_1d, dwd2dwsinit.dws_trade_user_sku_order_refund_1d,
                 dwd2dwsinit.dws_trade_user_order_1d, dwd2dwsinit.dws_trade_user_cart_add_1d,
                 dwd2dwsinit.dws_trade_user_payment_1d, dwd2dwsinit.dws_trade_user_order_refund_1d,
                 dwd2dwsinit.dws_trade_province_order_1d, dwd2dwsinit.dws_traffic_session_page_view_1d,
                 dwd2dwsinit.dws_traffic_page_visitor_page_view_1d, dwd2dwsinit.dws_trade_user_sku_order_nd,
                 dwd2dwsinit.dws_trade_user_sku_order_refund_nd, dwd2dwsinit.dws_trade_user_order_nd,
                 dwd2dwsinit.dws_trade_user_cart_add_nd, dwd2dwsinit.dws_trade_user_payment_nd,
                 dwd2dwsinit.dws_trade_user_order_refund_nd, dwd2dwsinit.dws_trade_province_order_nd,
                 dwd2dwsinit.dws_trade_coupon_order_nd, dwd2dwsinit.dws_trade_activity_order_nd,
                 dwd2dwsinit.dws_traffic_page_visitor_page_view_nd, dwd2dwsinit.dws_trade_user_order_td,
                 dwd2dwsinit.dws_trade_user_payment_td, dwd2dwsinit.dws_user_user_login_td]

DWS_SQLS = [dwd2dws.dws_trade_user_sku_order_1d, dwd2dws.dws_trade_user_sku_order_refund_1d,
            dwd2dws.dws_trade_user_order_1d, dwd2dws.dws_trade_user_cart_add_1d, dwd2dws.dws_trade_user_payment_1d,
            dwd2dws.dws_trade_user_order_refund_1d, dwd2dws.dws_trade_province_order_1d,
            dwd2dws.dws_traffic_session_page_view_1d, dwd2dws.dws_traffic_page_visitor_page_view_1d,
            dwd2dws.dws_trade_user_sku_order_nd, dwd2dws.dws_trade_user_sku_order_refund_nd,
            dwd2dws.dws_trade_user_order_nd, dwd2dws.dws_trade_user_cart_add_nd, dwd2dws.dws_trade_user_payment_nd,
            dwd2dws.dws_trade_user_order_refund_nd, dwd2dws.dws_trade_province_order_nd,
            dwd2dws.dws_trade_coupon_order_nd, dwd2dws.dws_trade_activity_order_nd,
            dwd2dws.dws_traffic_page_visitor_page_view_nd, dwd2dws.dws_trade_user_order_td,
            dwd2dws.dws_trade_user_payment_td, dwd2dws.dws_user_user_login_td]

ADS_SQLS = [dws2ads.ads_traffic_stats_by_channel, dws2ads.ads_page_path, dws2ads.ads_user_change,
            dws2ads.ads_user_retention, dws2ads.ads_user_stats, dws2ads.ads_user_action, dws2ads.ads_new_buyer_stats,
            dws2ads.ads_repeat_purchase_by_tm, dws2ads.ads_trade_stats_by_tm, dws2ads.ads_trade_stats_by_cate,
            dws2ads.ads_sku_cart_num_top3_by_cate, dws2ads.ads_trade_stats, dws2ads.ads_order_by_province,
            dws2ads.ads_coupon_stats, dws2ads.ads_activity_stats]


# 生成全量数据加载到ods的sql
def generate_etl2ods_full_sql(datahouse_dir, start_date):
    SQL_LOAD_LOG = f"load data inpath '{datahouse_dir}{ETL_LOG_DIR}/{start_date}' into table {DATA_BASE}.ods_log_inc partition(dt='{start_date}');\n"
    SQL_FILE_DATA = SQL_LOAD_LOG
    # 从full目录下加载该表的所有文件,文件格式为 2024.03.25_0.parquet
    # 如果表数据量较小，一天同步完成，则可能包含同日期的多个分区文件，如,2024.03.26_0.parquet,,2024.03.26_1.parquet
    # 如果表数据量很大，一天同步完成不了，则可能包含多个日期的多个分区文件，如,2024.03.25_0.parquet,,2024.03.26_1.parquet
    for table in FULL_LOAD_TABLES:
        ods_table = f'ods_{table}_full'
        SQL_FILE_DATA += f"load data inpath '{datahouse_dir}{ETL_DB_FULL_DIR}/{table}' overwrite into table {DATA_BASE}.{ods_table} partition(dt='{start_date}');\n"

    # 将 SQL_FILE_DATA 写入文件
    with open(f"{SQL_FILE_DIR}/etl2ods_full.sql", 'w') as f:
        f.write(SQL_FILE_DATA)


# 生成增量数据加载到ods的sql
def generate_etl2ods_inc_sql(datahouse_dir, start_date):
    SQL_LOAD_LOG = f"load data inpath '{datahouse_dir}{ETL_LOG_DIR}/{start_date}' into table {DATA_BASE}.ods_log_inc partition(dt='{start_date}');\n"
    SQL_FILE_DATA = SQL_LOAD_LOG

    # 从Inc目录下加载该表的所有日期为start_date的文件,文件格式为 2024.03.25_0.parquet，2024.03.25_1.parquet等等
    for table in INC_LOAD_TABLES:
        ods_table = f'ods_{table}_inc'
        # 获取{ETL_DB_INC_DIR}/{table}这个目录下以{start_date}为前缀的文件列表，使用通配符*实现
        SQL_FILE_DATA += f"load data inpath '{datahouse_dir}{ETL_DB_INC_DIR}/{table}/{start_date}*' into table {DATA_BASE}.{ods_table} partition(dt='{start_date}');\n"

    # 将 SQL_FILE_DATA 写入文件
    with open(f"{SQL_FILE_DIR}/etl2ods_inc.sql", 'w') as f:
        f.write(SQL_FILE_DATA)


# 生成初始化事实表的sql
def generate_ods2dwd_init_sql(start_date):
    ods2dwdinit.start_date = start_date
    SQL_FILE_DATA = ""
    for sql in DWD_INIT_SQLS:
        SQL_FILE_DATA += sql

    # 将 SQL_FILE_DATA 写入文件
    with open(f"{SQL_FILE_DIR}/ods2dwd_init.sql", 'w') as f:
        f.write(SQL_FILE_DATA)


# 生成初始化维度表的sql
def generate_ods2dim_init_sql(start_date):
    ods2diminit.start_date = start_date
    SQL_FILE_DATA = ""
    for sql in DIM_INIT_SQLS:
        SQL_FILE_DATA += sql

    # 将 SQL_FILE_DATA 写入文件
    with open(f"{SQL_FILE_DIR}/ods2dim_init.sql", 'w') as f:
        f.write(SQL_FILE_DATA)


# 生成维度表的sql
def generate_ods2dim_sql(start_date):
    ods2dim.start_date = start_date
    SQL_FILE_DATA = ""
    for sql in DIM_SQLS:
        SQL_FILE_DATA += sql

    # 将 SQL_FILE_DATA 写入文件
    with open(f"{SQL_FILE_DIR}/ods2dim.sql", 'w') as f:
        f.write(SQL_FILE_DATA)


# 生成事实表的sql
def generate_ods2dwd_sql(start_date):
    ods2dwd.start_date = start_date
    SQL_FILE_DATA = ""
    for sql in DWD_SQLS:
        SQL_FILE_DATA += sql

    # 将 SQL_FILE_DATA 写入文件
    with open(f"{SQL_FILE_DIR}/ods2dwd.sql", 'w') as f:
        f.write(SQL_FILE_DATA)


# 生成初始化宽表的sql
def generate_dwd2dws_init_sql(start_date):
    dwd2dwsinit.start_date = start_date
    SQL_FILE_DATA = ""
    for sql in DWS_INIT_SQLS:
        SQL_FILE_DATA += sql

    # 将 SQL_FILE_DATA 写入文件
    with open(f"{SQL_FILE_DIR}/dwd2dws_init.sql", 'w') as f:
        f.write(SQL_FILE_DATA)


# 生成宽表的sql
def generate_dwd2dws_sql(start_date):
    dwd2dws.start_date = start_date
    SQL_FILE_DATA = ""
    for sql in DWS_SQLS:
        SQL_FILE_DATA += sql

    # 将 SQL_FILE_DATA 写入文件
    with open(f"{SQL_FILE_DIR}/dwd2dws.sql", 'w') as f:
        f.write(SQL_FILE_DATA)


# 生成应用表的sql
def generate_dws2ads_sql(start_date):
    dws2ads.start_date = start_date
    SQL_FILE_DATA = ""
    for sql in ADS_SQLS:
        SQL_FILE_DATA += sql

    # 将 SQL_FILE_DATA 写入文件
    with open(f"{SQL_FILE_DIR}/dws2ads.sql", 'w') as f:
        f.write(SQL_FILE_DATA)


if __name__ == '__main__':
    MINIO_DATAHOUSE_DIR = "s3a://nineinfra/datahouse"
    generate_etl2ods_full_sql(MINIO_DATAHOUSE_DIR, datetime.date(2024, 3, 25))
    generate_etl2ods_inc_sql(MINIO_DATAHOUSE_DIR, datetime.date(2024, 3, 25))
    generate_ods2dwd_init_sql(datetime.date(2024, 3, 25))
    generate_ods2dim_init_sql(datetime.date(2024, 3, 25))
    generate_ods2dim_sql(datetime.date(2024, 3, 25))
    generate_ods2dwd_sql(datetime.date(2024, 3, 25))
    generate_dwd2dws_init_sql(datetime.date(2024, 3, 25))
    generate_dwd2dws_sql(datetime.date(2024, 3, 25))
    generate_dws2ads_sql(datetime.date(2024, 3, 25))
