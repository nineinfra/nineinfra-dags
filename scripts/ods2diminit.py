import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__)))

DATA_BASE = "datahouse"
start_date = "2024-03-27"

dim_date = f"\
    load data local inpath 'date_info.tsv' overwrite into table ${DATA_BASE}.tmp_dim_date_info;\
    insert overwrite table ${DATA_BASE}.dim_date select * from ${DATA_BASE}.tmp_dim_date_info;"

dim_user_zip = f"\
    insert overwrite table ${DATA_BASE}.dim_user_zip\
    partition (dt = '9999-12-31')\
    select data.id,\
           data.login_name,\
           data.nick_name,\
           md5(data.name),\
           md5(data.phone_num),\
           md5(data.email),\
           data.user_level,\
           data.birthday,\
           data.gender,\
           data.create_time,\
           data.operate_time,\
           '${start_date}'          as start_date,\
           '9999-12-31'          as end_date\
    from ${DATA_BASE}.ods_user_info_inc\
    where dt = '${start_date}' and type='bootstrap-insert';"

dim_sku_full = f"\
    with sku as\
    (\
        select id,\
               price,\
               sku_name,\
               sku_desc,\
               weight,\
               is_sale,\
               spu_id,\
               category3_id,\
               tm_id,\
               create_time\
        from ${DATA_BASE}.ods_sku_info_full\
        where dt = '${start_date}'\
    ),\
    spu as\
    (\
        select id,\
               spu_name\
        from ${DATA_BASE}.ods_spu_info_full\
        where dt = '${start_date}'\
    ),\
    c3 as\
    (\
        select id,\
               name,\
               category2_id\
        from ${DATA_BASE}.ods_base_category3_full\
        where dt = '${start_date}'\
    ),\
    c2 as\
    (\
        select id,\
               name,\
               category1_id\
        from ${DATA_BASE}.ods_base_category2_full\
        where dt = '${start_date}'\
    ),\
    c1 as\
    (\
        select id,\
               name\
        from ${DATA_BASE}.ods_base_category1_full\
        where dt = '${start_date}'\
    ),\
    tm as\
    (\
        select id,\
               tm_name\
        from ${DATA_BASE}.ods_base_trademark_full\
        where dt = '${start_date}'\
    ),\
    attr as\
    (\
        select sku_id,\
               collect_set\
               (\
                   named_struct\
                   (\
                       'attr_id',    attr_id,\
                       'value_id',   value_id,\
                       'attr_name',  attr_name,\
                       'value_name', value_name\
                   )\
               )                 as attrs\
        from ${DATA_BASE}.ods_sku_attr_value_full\
        where dt = '${start_date}'\
        group by sku_id\
    ),\
    sale_attr as\
    (\
        select sku_id,\
               collect_set\
               (\
                   named_struct\
                   (\
                       'sale_attr_id',         sale_attr_id,\
                       'sale_attr_value_id',   sale_attr_value_id,\
                       'sale_attr_name',       sale_attr_name,\
                       'sale_attr_value_name', sale_attr_value_name\
                   )\
               )                as sale_attrs\
        from ${DATA_BASE}.ods_sku_sale_attr_value_full\
        where dt = '${start_date}'\
        group by sku_id\
    )\
    insert overwrite table ${DATA_BASE}.dim_sku_full\
    partition(dt = '${start_date}')\
    select\
        sku.id,\
        sku.price,\
        sku.sku_name,\
        sku.sku_desc,\
        sku.weight,\
        sku.is_sale,\
        sku.spu_id,\
        spu.spu_name,\
        sku.category3_id,\
        c3.name,\
        c3.category2_id,\
        c2.name,\
        c2.category1_id,\
        c1.name,\
        sku.tm_id,\
        tm.tm_name,\
        attr.attrs,\
        sale_attr.sale_attrs,\
        sku.create_time\
    from sku left join spu      on sku.spu_id       = spu.id\
             left join c3       on sku.category3_id = c3.id\
             left join c2       on c3.category2_id  = c2.id\
             left join c1       on c2.category1_id  = c1.id\
             left join tm       on sku.tm_id        = tm.id\
             left join attr     on sku.id           = attr.sku_id\
             left join sale_attr on sku.id          = sale_attr.sku_id;"

dim_province_full = f"\
    insert overwrite table ${DATA_BASE}.dim_province_full partition(dt = '${start_date}')\
    select province.id,\
           province.name,\
           province.area_code,\
           province.iso_code,\
           province.iso_3166_2,\
           region_id,\
           region_name\
    from\
    (\
        select id,\
               name,\
               region_id,\
               area_code,\
               iso_code,\
               iso_3166_2\
        from ${DATA_BASE}.ods_base_province_full\
        where dt = '${start_date}'\
    ) as province left join\
    (\
        select id,\
               region_name\
        from ${DATA_BASE}.ods_base_region_full\
        where dt = '${start_date}'\
    ) as region\
        on province.region_id = region.id;"

dim_coupon_full = f"\
    insert overwrite table ${DATA_BASE}.dim_coupon_full partition(dt = '${start_date}')\
        select id,\
               coupon_name,\
               coupon_type,\
               coupon_dic.dic_name,\
               condition_amount,\
               condition_num,\
               activity_id,\
               benefit_amount,\
               benefit_discount,\
               case coupon_type\
                   when '3201' then concat('满 ', condition_amount, ' 元减 ', benefit_amount, ' 元')\
                   when '3202' then concat('满 ', condition_num,    ' 件打 ', 10 * (1 - benefit_discount), ' 折')\
                   when '3203' then concat('减 ', benefit_amount,   ' 元')\
               end benefit_rule,\
               create_time,\
               range_type,\
               range_dic.dic_name,\
               limit_num,\
               taken_count,\
               start_time,\
               end_time,\
               operate_time,\
               expire_time\
        from\
        (\
            select id,\
                   coupon_name,\
                   coupon_type,\
                   condition_amount,\
                   condition_num,\
                   activity_id,\
                   benefit_amount,\
                   benefit_discount,\
                   create_time,\
                   range_type,\
                   limit_num,\
                   taken_count,\
                   start_time,\
                   end_time,\
                   operate_time,\
                   expire_time\
            from ${DATA_BASE}.ods_coupon_info_full\
            where dt = '${start_date}'\
        ) ci left join\
        (\
            select dic_code,\
                   dic_name\
            from ${DATA_BASE}.ods_base_dic_full\
            where dt = '${start_date}' and parent_code = '32'\
        ) coupon_dic on ci.coupon_type=coupon_dic.dic_code left join\
        (\
            select dic_code,\
                   dic_name\
            from ${DATA_BASE}.ods_base_dic_full\
            where dt = '${start_date}' and parent_code = '33'\
        ) range_dic on ci.range_type=range_dic.dic_code;"

dim_activity_full = f"\
    insert overwrite table ${DATA_BASE}.dim_activity_full partition(dt = '${start_date}')\
    select rule.id,\
           info.id,\
           activity_name,\
           rule.activity_type,\
           dic.dic_name,\
           activity_desc,\
           start_time,\
           end_time,\
           create_time,\
           condition_amount,\
           condition_num,\
           benefit_amount,\
           benefit_discount,\
           case rule.activity_type\
               when '3101' then concat('满 ', condition_amount,            '元减 ', benefit_amount,              ' 元')\
               when '3102' then concat('满 ', condition_num,               '件打 ', 10 * (1 - benefit_discount), ' 折')\
               when '3103' then concat('打 ', 10 * (1 - benefit_discount), ' 折')\
           end benefit_rule,\
           benefit_level\
    from\
    (\
        select id,\
               activity_id,\
               activity_type,\
               condition_amount,\
               condition_num,\
               benefit_amount,\
               benefit_discount,\
               benefit_level\
        from ${DATA_BASE}.ods_activity_rule_full\
        where dt = '${start_date}'\
    ) as rule left join\
    (\
        select id,\
               activity_name,\
               activity_type,\
               activity_desc,\
               start_time,\
               end_time,\
               create_time\
        from ${DATA_BASE}.ods_activity_info_full\
        where dt = '${start_date}'\
    ) as info\
        on rule.activity_id = info.id\
    left join\
    (\
        select dic_code,\
               dic_name\
        from ${DATA_BASE}.ods_base_dic_full\
        where dt = '${start_date}' and parent_code = '31'\
    ) as dic\
        on rule.activity_type = dic.dic_code;"
