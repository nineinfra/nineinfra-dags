import os
import sys

sys.path.append(os.path.join(os.path.dirname(__file__)))

datahouse_dir = "s3a://nineinfra/datahouse"
DATA_BASE = "datahouse"
start_date = "2024-03-27"

dim_date = f"\
    load data inpath '{datahouse_dir}/common/date_info.tsv' overwrite into table {DATA_BASE}.tmp_dim_date_info;\n\
    insert overwrite table {DATA_BASE}.dim_date select * from {DATA_BASE}.tmp_dim_date_info;"

dim_user_zip = f"\n\
    insert overwrite table {DATA_BASE}.dim_user_zip\n\
    partition (dt = '9999-12-31')\n\
    select data.id,\n\
           data.login_name,\n\
           data.nick_name,\n\
           md5(data.name),\n\
           md5(data.phone_num),\n\
           md5(data.email),\n\
           data.user_level,\n\
           data.birthday,\n\
           data.gender,\n\
           data.create_time,\n\
           data.operate_time,\n\
           '{start_date}'          as start_date,\n\
           '9999-12-31'          as end_date\n\
    from {DATA_BASE}.ods_user_info_inc\n\
    where dt = '{start_date}' and type='bootstrap-insert';"

dim_sku_full = f"\n\
    with sku as\n\
    (\n\
        select id,\n\
               price,\n\
               sku_name,\n\
               sku_desc,\n\
               weight,\n\
               CASE WHEN is_sale = '1' THEN true\n\
                    WHEN is_sale = '0' THEN false\n\
               END AS is_sale,\n\
               spu_id,\n\
               category3_id,\n\
               tm_id,\n\
               create_time\n\
        from {DATA_BASE}.ods_sku_info_full\n\
        where dt = '{start_date}'\n\
    ),\n\
    spu as\n\
    (\n\
        select id,\n\
               spu_name\n\
        from {DATA_BASE}.ods_spu_info_full\n\
        where dt = '{start_date}'\n\
    ),\n\
    c3 as\n\
    (\n\
        select id,\n\
               name,\n\
               category2_id\n\
        from {DATA_BASE}.ods_base_category3_full\n\
        where dt = '{start_date}'\n\
    ),\n\
    c2 as\n\
    (\n\
        select id,\n\
               name,\n\
               category1_id\n\
        from {DATA_BASE}.ods_base_category2_full\n\
        where dt = '{start_date}'\n\
    ),\n\
    c1 as\n\
    (\n\
        select id,\n\
               name\n\
        from {DATA_BASE}.ods_base_category1_full\n\
        where dt = '{start_date}'\n\
    ),\n\
    tm as\n\
    (\n\
        select id,\n\
               tm_name\n\
        from {DATA_BASE}.ods_base_trademark_full\n\
        where dt = '{start_date}'\n\
    ),\n\
    attr as\n\
    (\n\
        select sku_id,\n\
               collect_set\n\
               (\n\
                   named_struct\n\
                   (\n\
                       'attr_id',    attr_id,\n\
                       'value_id',   value_id,\n\
                       'attr_name',  attr_name,\n\
                       'value_name', value_name\n\
                   )\n\
               )                 as attrs\n\
        from {DATA_BASE}.ods_sku_attr_value_full\n\
        where dt = '{start_date}'\n\
        group by sku_id\n\
    ),\n\
    sale_attr as\n\
    (\n\
        select sku_id,\n\
               collect_set\n\
               (\n\
                   named_struct\n\
                   (\n\
                       'sale_attr_id',         sale_attr_id,\n\
                       'sale_attr_value_id',   sale_attr_value_id,\n\
                       'sale_attr_name',       sale_attr_name,\n\
                       'sale_attr_value_name', sale_attr_value_name\n\
                   )\n\
               )                as sale_attrs\n\
        from {DATA_BASE}.ods_sku_sale_attr_value_full\n\
        where dt = '{start_date}'\n\
        group by sku_id\n\
    )\n\
    insert overwrite table {DATA_BASE}.dim_sku_full\n\
    partition(dt = '{start_date}')\n\
    select\n\
        sku.id,\n\
        sku.price,\n\
        sku.sku_name,\n\
        sku.sku_desc,\n\
        sku.weight,\n\
        sku.is_sale,\n\
        sku.spu_id,\n\
        spu.spu_name,\n\
        sku.category3_id,\n\
        c3.name,\n\
        c3.category2_id,\n\
        c2.name,\n\
        c2.category1_id,\n\
        c1.name,\n\
        sku.tm_id,\n\
        tm.tm_name,\n\
        attr.attrs,\n\
        sale_attr.sale_attrs,\n\
        sku.create_time\n\
    from sku left join spu      on sku.spu_id       = spu.id\n\
             left join c3       on sku.category3_id = c3.id\n\
             left join c2       on c3.category2_id  = c2.id\n\
             left join c1       on c2.category1_id  = c1.id\n\
             left join tm       on sku.tm_id        = tm.id\n\
             left join attr     on sku.id           = attr.sku_id\n\
             left join sale_attr on sku.id          = sale_attr.sku_id;"

dim_province_full = f"\n\
    insert overwrite table {DATA_BASE}.dim_province_full partition(dt = '{start_date}')\n\
    select province.id,\n\
           province.name,\n\
           province.area_code,\n\
           province.iso_code,\n\
           province.iso_3166_2,\n\
           region_id,\n\
           region_name\n\
    from\n\
    (\n\
        select id,\n\
               name,\n\
               region_id,\n\
               area_code,\n\
               iso_code,\n\
               iso_3166_2\n\
        from {DATA_BASE}.ods_base_province_full\n\
        where dt = '{start_date}'\n\
    ) as province left join\n\
    (\n\
        select id,\n\
               region_name\n\
        from {DATA_BASE}.ods_base_region_full\n\
        where dt = '{start_date}'\n\
    ) as region\n\
        on province.region_id = region.id;"

dim_coupon_full = f"\n\
    insert overwrite table {DATA_BASE}.dim_coupon_full partition(dt = '{start_date}')\n\
        select id,\n\
               coupon_name,\n\
               coupon_type,\n\
               coupon_dic.dic_name,\n\
               condition_amount,\n\
               condition_num,\n\
               activity_id,\n\
               benefit_amount,\n\
               benefit_discount,\n\
               case coupon_type\n\
                   when '3201' then concat('满 ', condition_amount, ' 元减 ', benefit_amount, ' 元')\n\
                   when '3202' then concat('满 ', condition_num,    ' 件打 ', 10 * (1 - benefit_discount), ' 折')\n\
                   when '3203' then concat('减 ', benefit_amount,   ' 元')\n\
               end benefit_rule,\n\
               create_time,\n\
               range_type,\n\
               range_dic.dic_name,\n\
               limit_num,\n\
               taken_count,\n\
               start_time,\n\
               end_time,\n\
               operate_time,\n\
               expire_time\n\
        from\n\
        (\n\
            select id,\n\
                   coupon_name,\n\
                   coupon_type,\n\
                   condition_amount,\n\
                   condition_num,\n\
                   activity_id,\n\
                   benefit_amount,\n\
                   benefit_discount,\n\
                   create_time,\n\
                   range_type,\n\
                   limit_num,\n\
                   taken_count,\n\
                   start_time,\n\
                   end_time,\n\
                   operate_time,\n\
                   expire_time\n\
            from {DATA_BASE}.ods_coupon_info_full\n\
            where dt = '{start_date}'\n\
        ) ci left join\n\
        (\n\
            select dic_code,\n\
                   dic_name\n\
            from {DATA_BASE}.ods_base_dic_full\n\
            where dt = '{start_date}' and parent_code = '32'\n\
        ) coupon_dic on ci.coupon_type=coupon_dic.dic_code left join\n\
        (\n\
            select dic_code,\n\
                   dic_name\n\
            from {DATA_BASE}.ods_base_dic_full\n\
            where dt = '{start_date}' and parent_code = '33'\n\
        ) range_dic on ci.range_type=range_dic.dic_code;"

dim_activity_full = f"\n\
    insert overwrite table {DATA_BASE}.dim_activity_full partition(dt = '{start_date}')\n\
    select rule.id,\n\
           info.id,\n\
           activity_name,\n\
           rule.activity_type,\n\
           dic.dic_name,\n\
           activity_desc,\n\
           start_time,\n\
           end_time,\n\
           create_time,\n\
           condition_amount,\n\
           condition_num,\n\
           benefit_amount,\n\
           benefit_discount,\n\
           case rule.activity_type\n\
               when '3101' then concat('满 ', condition_amount,            '元减 ', benefit_amount,              ' 元')\n\
               when '3102' then concat('满 ', condition_num,               '件打 ', 10 * (1 - benefit_discount), ' 折')\n\
               when '3103' then concat('打 ', 10 * (1 - benefit_discount), ' 折')\n\
           end benefit_rule,\n\
           benefit_level\n\
    from\n\
    (\n\
        select id,\n\
               activity_id,\n\
               activity_type,\n\
               condition_amount,\n\
               condition_num,\n\
               benefit_amount,\n\
               benefit_discount,\n\
               benefit_level\n\
        from {DATA_BASE}.ods_activity_rule_full\n\
        where dt = '{start_date}'\n\
    ) as rule left join\n\
    (\n\
        select id,\n\
               activity_name,\n\
               activity_type,\n\
               activity_desc,\n\
               start_time,\n\
               end_time,\n\
               create_time\n\
        from {DATA_BASE}.ods_activity_info_full\n\
        where dt = '{start_date}'\n\
    ) as info\n\
        on rule.activity_id = info.id\n\
    left join\n\
    (\n\
        select dic_code,\n\
               dic_name\n\
        from {DATA_BASE}.ods_base_dic_full\n\
        where dt = '{start_date}' and parent_code = '31'\n\
    ) as dic\n\
        on rule.activity_type = dic.dic_code;"
