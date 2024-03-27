#!/usr/bin/env bash

# =========================================================================================
#    FileName      ：  ods-dim.sh
#    CreateTime    ：  2023-03-26 01:44
#    Author        ：  lihua shiyu
#    Email         ：  lihuashiyu@github.com
#    Description   ：  ods-dim.sh 被用于 ==> 将 ODS 层数据加载到 DIM 
# =========================================================================================
    
    
# 定义变量方便修改
SERVICE_DIR=$(cd "$(dirname "$0")" || exit; pwd)           # 服务位置
HIVE_HOME=/opt/apache/hive                                 # Hive 的安装位置
HIVE_DATA_BASE=warehouse                                   # Hive 的数据库名称
LOG_FILE="ods-dim-$(date +%F).log"                         # 执行日志


# 如果是输入的日期按照取输入日期；如果没输入日期取当前时间的前一天
if [ -n "$2" ]; then
    do_date=$2
else
    do_date=$(date -d "-1 day" +%F)
fi

dim_user_zip="
    set hive.exec.dynamic.partition.mode=nonstrict;
    with tmp as 
    (
        select old.id           as old_id,
               old.login_name   as old_login_name,
               old.nick_name    as old_nick_name,
               old.name         as old_name,
               old.phone_num    as old_phone_num,
               old.email        as old_email,
               old.user_level   as old_user_level,
               old.birthday     as old_birthday,
               old.gender       as old_gender,
               old.create_time  as old_create_time,
               old.operate_time as old_operate_time,
               old.start_date   as old_start_date,
               old.end_date     as old_end_date,
               new.id           as new_id,
               new.login_name   as new_login_name,
               new.nick_name    as new_nick_name,
               new.name         as new_name,
               new.phone_num    as new_phone_num,
               new.email        as new_email,
               new.user_level   as new_user_level,
               new.birthday     as new_birthday,
               new.gender       as new_gender,
               new.create_time  as new_create_time,
               new.operate_time as new_operate_time,
               new.start_date   as new_start_date,
               new.end_date     as new_end_date
        from 
        (
            select id,
                   login_name,
                   nick_name,
                   name,
                   phone_num,
                   email,
                   user_level,
                   birthday,
                   gender,
                   create_time,
                   operate_time,
                   start_date,
                   end_date
            from ${HIVE_DATA_BASE}.dim_user_zip
            where dt = '9999-12-31'
        ) as old full outer join 
        (
            select id,
                   login_name,
                   nick_name,
                   md5(name)      as name,
                   md5(phone_num) as phone_num,
                   md5(email)     as email,
                   user_level,
                   birthday,
                   gender,
                   create_time,
                   operate_time,
                   '${do_date}'   as start_date,
                   '9999-12-31'   as end_date    
	        from     
	        (    
	            select data.id,    
	                   data.login_name,    
	                   data.nick_name,    
	                   data.name,    
	                   data.phone_num,    
	                   data.email,    
	                   data.user_level,    
	                   data.birthday,    
	                   data.gender,    
	                   data.create_time,    
	                   data.operate_time,    
	                   row_number() over (partition by data.id order by ts desc) as rn    
		        from ${HIVE_DATA_BASE}.ods_user_info_inc    
		        where dt = '${do_date}'    
	        ) as t1 where rn = 1
        ) as new on old.id = new.id
    )
    insert overwrite table dim_user_zip partition(dt)
    select if(new_id is not null, new_id,           old_id),
           if(new_id is not null, new_login_name,   old_login_name),
           if(new_id is not null, new_nick_name,    old_nick_name),
           if(new_id is not null, new_name,         old_name),
           if(new_id is not null, new_phone_num,    old_phone_num),
           if(new_id is not null, new_email,        old_email),
           if(new_id is not null, new_user_level,   old_user_level),
           if(new_id is not null, new_birthday,     old_birthday),
           if(new_id is not null, new_gender,       old_gender),
           if(new_id is not null, new_create_time,  old_create_time),
           if(new_id is not null, new_operate_time, old_operate_time),
           if(new_id is not null, new_start_date,   old_start_date),
           if(new_id is not null, new_end_date,     old_end_date),
           if(new_id is not null, new_end_date,     old_end_date) dt
    from tmp 
    union all
    select old_id,
           old_login_name,
           old_nick_name,
           old_name,
           old_phone_num,
           old_email,
           old_user_level,
           old_birthday,
           old_gender,
           old_create_time,
           old_operate_time,
           old_start_date,
           cast(date_add('${do_date}', -1) as string) as old_end_date,
           cast(date_add('${do_date}', -1) as string) as dt
    from tmp
    where old_id is not null and new_id is not null;
    "

dim_sku_full="
    with sku as
    (
        select id,
               price,
               sku_name,
               sku_desc,
               weight,
               is_sale,
               spu_id,
               category3_id,
               tm_id,
               create_time
        from ${HIVE_DATA_BASE}.ods_sku_info_full
        where dt = '${do_date}'
    ),
    spu as
    (
        select id,
               spu_name
        from ${HIVE_DATA_BASE}.ods_spu_info_full
        where dt = '${do_date}'
    ),
    c3 as
    (
        select id,
               name,
               category2_id
        from ${HIVE_DATA_BASE}.ods_base_category3_full
        where dt = '${do_date}'
    ),
    c2 as
    (
        select id,
               name,
               category1_id
        from ${HIVE_DATA_BASE}.ods_base_category2_full
        where dt = '${do_date}'
    ),
    c1 as
    (
        select id,
               name
        from ${HIVE_DATA_BASE}.ods_base_category1_full
        where dt = '${do_date}'
    ),
    tm as
    (
        select id,
               tm_name
        from ${HIVE_DATA_BASE}.ods_base_trademark_full
        where dt = '${do_date}'
    ),
    attr as
    (
        select sku_id,
               collect_set
               (
                   named_struct
                   (
                       'attr_id',    attr_id, 
                       'value_id',   value_id, 
                       'attr_name',  attr_name, 
                       'value_name', value_name
                   )
               )                 as attrs
        from ${HIVE_DATA_BASE}.ods_sku_attr_value_full
        where dt = '${do_date}'
        group by sku_id
    ),
    sale_attr as
    (
        select sku_id,
               collect_set
               (
                   named_struct
                   (  
                       'sale_attr_id',         sale_attr_id, 
                       'sale_attr_value_id',   sale_attr_value_id, 
                       'sale_attr_name',       sale_attr_name, 
                       'sale_attr_value_name', sale_attr_value_name
                   )
               )                as sale_attrs
        from ${HIVE_DATA_BASE}.ods_sku_sale_attr_value_full
        where dt = '${do_date}'
        group by sku_id
    )
    insert overwrite table ${HIVE_DATA_BASE}.dim_sku_full partition(dt = '${do_date}')
        select sku.id,
            sku.price,
            sku.sku_name,
            sku.sku_desc,
            sku.weight,
            sku.is_sale,
            sku.spu_id,
            spu.spu_name,
            sku.category3_id,
            c3.name,
            c3.category2_id,
            c2.name,
            c2.category1_id,
            c1.name,
            sku.tm_id,
            tm.tm_name,
            attr.attrs,
            sale_attr.sale_attrs,
            sku.create_time
        from sku left join spu       on sku.spu_id       = spu.id
                 left join c3        on sku.category3_id = c3.id
                 left join c2        on c3.category2_id  = c2.id
                 left join c1        on c2.category1_id  = c1.id
                 left join tm        on sku.tm_id        = tm.id
                 left join attr      on sku.id           = attr.sku_id
                 left join sale_attr on sku.id           = sale_attr.sku_id;
    "

dim_province_full="
insert overwrite table ${HIVE_DATA_BASE}.dim_province_full partition(dt = '${do_date}')
    select province.id,
           province.name,
           province.area_code,
           province.iso_code,
           province.iso_3166_2,
           region_id,
           region_name
    from
    (
        select id,
               name,
               region_id,
               area_code,
               iso_code,
               iso_3166_2
        from ${HIVE_DATA_BASE}.ods_base_province_full
        where dt = '${do_date}'
    ) province left join
    (
        select id,
               region_name
        from ${HIVE_DATA_BASE}.ods_base_region_full
        where dt = '${do_date}'
    ) region on province.region_id = region.id;
"

dim_coupon_full="
insert overwrite table ${HIVE_DATA_BASE}.dim_coupon_full partition(dt = '${do_date}')
    select id,
           coupon_name,
           coupon_type,
           coupon_dic.dic_name,
           condition_amount,
           condition_num,
           activity_id,
           benefit_amount,
           benefit_discount,
           case coupon_type
               when '3201' then concat('满 ', condition_amount, ' 元减 ', benefit_amount, ' 元')
               when '3202' then concat('满 ', condition_num,    ' 件打 ', 10 * (1 - benefit_discount), ' 折')
               when '3203' then concat('减 ', benefit_amount,   ' 元')
           end benefit_rule,
           create_time,
           range_type,
           range_dic.dic_name,
           limit_num,
           taken_count,
           start_time,
           end_time,
           operate_time,
           expire_time
    from
    (
        select id,
               coupon_name,
               coupon_type,
               condition_amount,
               condition_num,
               activity_id,
               benefit_amount,
               benefit_discount,
               create_time,
               range_type,
               limit_num,
               taken_count,
               start_time,
               end_time,
               operate_time,
               expire_time
        from ${HIVE_DATA_BASE}.ods_coupon_info_full
        where dt = '${do_date}'
    ) ci left join
    (
        select dic_code,
               dic_name
        from ${HIVE_DATA_BASE}.ods_base_dic_full
        where dt = '${do_date}' and parent_code = '32'
    ) coupon_dic on ci.coupon_type = coupon_dic.dic_code left join
    (
        select dic_code,
               dic_name
        from ${HIVE_DATA_BASE}.ods_base_dic_full
        where dt = '${do_date}' and parent_code = '33'
    ) range_dic on ci.range_type = range_dic.dic_code;
    "
    
dim_activity_full="
insert overwrite table ${HIVE_DATA_BASE}.dim_activity_full partition(dt = '${do_date}')
    select rule.id,
            info.id,
            activity_name,
            rule.activity_type,
            dic.dic_name,
            activity_desc,
            start_time,
            end_time,
            create_time,
            condition_amount,
            condition_num,
            benefit_amount,
            benefit_discount,
            case rule.activity_type
                when '3101' then concat('满 ', condition_amount,            ' 元减 ', benefit_amount,              ' 元')
                when '3102' then concat('满 ', condition_num,               ' 件打 ', 10 * (1 - benefit_discount), ' 折')
                when '3103' then concat('打 ', 10 * (1 - benefit_discount), ' 折')
            end benefit_rule,
            benefit_level
    from
    (
        select id,
               activity_id,
               activity_type,
               condition_amount,
               condition_num,
               benefit_amount,
               benefit_discount,
               benefit_level
        from ${HIVE_DATA_BASE}.ods_activity_rule_full
        where dt = '${do_date}'
    ) as rule left join
    (
        select id,
               activity_name,
               activity_type,
               activity_desc,
               start_time,
               end_time,
               create_time
        from ${HIVE_DATA_BASE}.ods_activity_info_full
        where dt='${do_date}'
    ) as info 
        on rule.activity_id = info.id left join
    (
        select dic_code, 
               dic_name
        from ${HIVE_DATA_BASE}.ods_base_dic_full
        where dt = '${do_date}' and parent_code = '31'
    ) as dic on rule.activity_type = dic.dic_code;
"

# 执行 Hive Sql
function execute_hive_sql()
{
    sql="$*"
    echo "${sql}" >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
    ${HIVE_HOME}/bin/hive -e "${sql}" >> "${SERVICE_DIR}/logs/${LOG_FILE}" 2>&1
}


printf "\n======================================== 数据加载开始 ========================================\n"
case $1 in
    dim_user_zip)
        execute_hive_sql "${dim_user_zip}"
    ;;
    
    dim_sku_full)
        execute_hive_sql "${dim_sku_full}"
    ;;
    
    dim_province_full)
        execute_hive_sql "${dim_province_full}"
    ;;
    
    dim_coupon_full)
        execute_hive_sql "${dim_coupon_full}"
    ;;
    
    dim_activity_full)
        execute_hive_sql "${dim_activity_full}"
    ;;
    
    all)
        execute_hive_sql "${dim_user_zip}"     "${dim_sku_full}"       "${dim_province_full}" \
                         "${dim_coupon_full}"  "${dim_activity_full}"
    ;;
    
    *)
        echo "    脚本可传入两个参数，使用方法：/path/$(basename $0) arg1 [arg2] ： "
        echo "        arg1：表名，必填，如下表所示；arg2：日期（yyyy-mm-dd），可选，默认昨天 "
        echo "        +---------------------+----------------+ "
        echo "        |       参   数       |     描  述     | "
        echo "        +---------------------+----------------+ "
        echo "        |  dim_sku_full       |  商品维度表    | "    
        echo "        |  dim_coupon_full    |  优惠券维度表  | "    
        echo "        |  dim_activity_full  |  活动维度表    | "    
        echo "        |  dim_province_full  |  地区维度表    | "        
        echo "        |  dim_user_zip       |  用户维度表    | "    
        echo "        |  all                |  所有 DIM 表   | "    
        echo "        +---------------------+----------------+ "
    ;;
esac

printf "======================================== 运行结束 ========================================\n\n"
exit 0
