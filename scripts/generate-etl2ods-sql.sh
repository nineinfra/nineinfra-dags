#!/bin/bash

DATA_BASE=datahouse                                   # Hive 的数据库名称
ETL_LOG_DIR=/kafka/nineinfra                                  # 原始文件在在 HDFS 上的路径
ETL_DB_DIR=/seatunnel/mysql
SQL_FILE_DIR=../sqls

DB_TYPE=mysql
FULL_LOAD_TABLES="ods_activity_info_full ods_activity_rule_full ods_base_category1_full ods_base_category2_full ods_base_category3_full ods_base_dic_full ods_base_province_full ods_base_region_full ods_base_trademark_full ods_cart_info_full ods_coupon_info_full ods_sku_attr_value_full ods_sku_info_full ods_sku_sale_attr_value_full ods_spu_info_full"
INC_LOAD_TABLES="ods_cart_info_inc ods_comment_info_inc ods_coupon_use_inc ods_favor_info_inc ods_order_detail_inc ods_order_detail_activity_inc ods_order_detail_coupon_inc ods_order_info_inc ods_order_refund_info_inc ods_order_status_log_inc ods_payment_info_inc ods_refund_payment_inc ods_user_info_inc"

# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天,夜里过12点开始跑批
if [ -n "$2" ] ;then
    do_date=$2
else
    do_date=$(date -d "-1 day" +%F)
fi

SQL_LOAD_LOG="load data inpath '${ETL_LOG_DIR}/${do_date}' into table ${DATA_BASE}.ods_log_inc partition(dt='${do_date}');"+'\n'
SQL_FILE_DATA=SQL_LOAD_LOG
for table in $FULL_LOAD_TABLES;
do
    SQL_FILE_DATA="${SQL_FILE_DATA} load data inpath '${ETL_DB_DIR}/${table}/${do_date}' overwrite into table ${DATA_BASE}.${table} partition(dt='${do_date}');"+ '\n'
done
for table in $INC_LOAD_TABLES;
do
    SQL_FILE_DATA="${SQL_FILE_DATA} load data inpath '${ETL_DB_DIR}/${table}/${do_date}' into table ${DATA_BASE}.${table} partition(dt='${do_date}');"+ '\n'
done

echo $SQL_FILE_DATA > ${SQL_FILE_DIR}/etl2ods.sql
exit 0
