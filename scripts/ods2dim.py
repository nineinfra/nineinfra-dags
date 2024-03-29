def get_ods2dim_sqls(data_base, start_date):
    dim_user_zip = f"\n\
        set hive.exec.dynamic.partition.mode=nonstrict;\n\
        with tmp as\n\
        (\n\
            select old.id           as old_id,\n\
                   old.login_name   as old_login_name,\n\
                   old.nick_name    as old_nick_name,\n\
                   old.name         as old_name,\n\
                   old.phone_num    as old_phone_num,\n\
                   old.email        as old_email,\n\
                   old.user_level   as old_user_level,\n\
                   old.birthday     as old_birthday,\n\
                   old.gender       as old_gender,\n\
                   old.create_time  as old_create_time,\n\
                   old.operate_time as old_operate_time,\n\
                   old.start_date   as old_start_date,\n\
                   old.end_date     as old_end_date,\n\
                   new.id           as new_id,\n\
                   new.login_name   as new_login_name,\n\
                   new.nick_name    as new_nick_name,\n\
                   new.name         as new_name,\n\
                   new.phone_num    as new_phone_num,\n\
                   new.email        as new_email,\n\
                   new.user_level   as new_user_level,\n\
                   new.birthday     as new_birthday,\n\
                   new.gender       as new_gender,\n\
                   new.create_time  as new_create_time,\n\
                   new.operate_time as new_operate_time,\n\
                   new.start_date   as new_start_date,\n\
                   new.end_date     as new_end_date\n\
            from\n\
            (\n\
                select id,\n\
                       login_name,\n\
                       nick_name,\n\
                       name,\n\
                       phone_num,\n\
                       email,\n\
                       user_level,\n\
                       birthday,\n\
                       gender,\n\
                       create_time,\n\
                       operate_time,\n\
                       start_date,\n\
                       end_date\n\
                from {data_base}.dim_user_zip\n\
                where dt = '9999-12-31'\n\
            ) as old full outer join\n\
            (\n\
                select id,\n\
                       login_name,\n\
                       nick_name,\n\
                       md5(name)      as name,\n\
                       md5(phone_num) as phone_num,\n\
                       md5(email)     as email,\n\
                       user_level,\n\
                       birthday,\n\
                       gender,\n\
                       create_time,\n\
                       operate_time,\n\
                       '{start_date}'   as start_date,\n\
                       '9999-12-31'   as end_date\n\
                from\n\
                (\n\
                    select data.id,\n\
                           data.login_name,\n\
                           data.nick_name,\n\
                           data.name,\n\
                           data.phone_num,\n\
                           data.email,\n\
                           data.user_level,\n\
                           data.birthday,\n\
                           data.gender,\n\
                           data.create_time,\n\
                           data.operate_time,\n\
                           row_number() over (partition by data.id order by ts desc) as rn\n\
                    from {data_base}.ods_user_info_inc\n\
                    where dt = '{start_date}'\n\
                ) as t1 where rn = 1\n\
            ) as new on old.id = new.id\n\
        )\n\
        insert overwrite table dim_user_zip partition(dt)\n\
        select if(new_id is not null, new_id,           old_id),\n\
               if(new_id is not null, new_login_name,   old_login_name),\n\
               if(new_id is not null, new_nick_name,    old_nick_name),\n\
               if(new_id is not null, new_name,         old_name),\n\
               if(new_id is not null, new_phone_num,    old_phone_num),\n\
               if(new_id is not null, new_email,        old_email),\n\
               if(new_id is not null, new_user_level,   old_user_level),\n\
               if(new_id is not null, new_birthday,     old_birthday),\n\
               if(new_id is not null, new_gender,       old_gender),\n\
               if(new_id is not null, new_create_time,  old_create_time),\n\
               if(new_id is not null, new_operate_time, old_operate_time),\n\
               if(new_id is not null, new_start_date,   old_start_date),\n\
               if(new_id is not null, new_end_date,     old_end_date),\n\
               if(new_id is not null, new_end_date,     old_end_date) dt\n\
        from tmp\n\
        union all\n\
        select old_id,\n\
               old_login_name,\n\
               old_nick_name,\n\
               old_name,\n\
               old_phone_num,\n\
               old_email,\n\
               old_user_level,\n\
               old_birthday,\n\
               old_gender,\n\
               old_create_time,\n\
               old_operate_time,\n\
               old_start_date,\n\
               cast(date_add('{start_date}', -1) as string) as old_end_date,\n\
               cast(date_add('{start_date}', -1) as string) as dt\n\
        from tmp\n\
        where old_id is not null and new_id is not null;"

    dim_sku_full = f"\n\
        with sku as\n\
        (\n\
            select id,\n\
                   price,\n\
                   sku_name,\n\
                   sku_desc,\n\
                   weight,\n\
                   is_sale,\n\
                   spu_id,\n\
                   category3_id,\n\
                   tm_id,\n\
                   create_time\n\
            from {data_base}.ods_sku_info_full\n\
            where dt = '{start_date}'\n\
        ),\n\
        spu as\n\
        (\n\
            select id,\n\
                   spu_name\n\
            from {data_base}.ods_spu_info_full\n\
            where dt = '{start_date}'\n\
        ),\n\
        c3 as\n\
        (\n\
            select id,\n\
                   name,\n\
                   category2_id\n\
            from {data_base}.ods_base_category3_full\n\
            where dt = '{start_date}'\n\
        ),\n\
        c2 as\n\
        (\n\
            select id,\n\
                   name,\n\
                   category1_id\n\
            from {data_base}.ods_base_category2_full\n\
            where dt = '{start_date}'\n\
        ),\n\
        c1 as\n\
        (\n\
            select id,\n\
                   name\n\
            from {data_base}.ods_base_category1_full\n\
            where dt = '{start_date}'\n\
        ),\n\
        tm as\n\
        (\n\
            select id,\n\
                   tm_name\n\
            from {data_base}.ods_base_trademark_full\n\
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
            from {data_base}.ods_sku_attr_value_full\n\
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
            from {data_base}.ods_sku_sale_attr_value_full\n\
            where dt = '{start_date}'\n\
            group by sku_id\n\
        )\n\
        insert overwrite table {data_base}.dim_sku_full partition(dt = '{start_date}')\n\
            select sku.id,\n\
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
            from sku left join spu       on sku.spu_id       = spu.id\n\
                     left join c3        on sku.category3_id = c3.id\n\
                     left join c2        on c3.category2_id  = c2.id\n\
                     left join c1        on c2.category1_id  = c1.id\n\
                     left join tm        on sku.tm_id        = tm.id\n\
                     left join attr      on sku.id           = attr.sku_id\n\
                     left join sale_attr on sku.id           = sale_attr.sku_id;"

    dim_province_full = f"\n\
    insert overwrite table {data_base}.dim_province_full partition(dt = '{start_date}')\n\
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
            from {data_base}.ods_base_province_full\n\
            where dt = '{start_date}'\n\
        ) province left join\n\
        (\n\
            select id,\n\
                   region_name\n\
            from {data_base}.ods_base_region_full\n\
            where dt = '{start_date}'\n\
        ) region on province.region_id = region.id;"

    dim_coupon_full = f"\n\
    insert overwrite table {data_base}.dim_coupon_full partition(dt = '{start_date}')\n\
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
            from {data_base}.ods_coupon_info_full\n\
            where dt = '{start_date}'\n\
        ) ci left join\n\
        (\n\
            select dic_code,\n\
                   dic_name\n\
            from {data_base}.ods_base_dic_full\n\
            where dt = '{start_date}' and parent_code = '32'\n\
        ) coupon_dic on ci.coupon_type = coupon_dic.dic_code left join\n\
        (\n\
            select dic_code,\n\
                   dic_name\n\
            from {data_base}.ods_base_dic_full\n\
            where dt = '{start_date}' and parent_code = '33'\n\
        ) range_dic on ci.range_type = range_dic.dic_code;"

    dim_activity_full = f"\n\
    insert overwrite table {data_base}.dim_activity_full partition(dt = '{start_date}')\n\
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
                    when '3101' then concat('满 ', condition_amount,            ' 元减 ', benefit_amount,              ' 元')\n\
                    when '3102' then concat('满 ', condition_num,               ' 件打 ', 10 * (1 - benefit_discount), ' 折')\n\
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
            from {data_base}.ods_activity_rule_full\n\
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
            from {data_base}.ods_activity_info_full\n\
            where dt='{start_date}'\n\
        ) as info\n\
            on rule.activity_id = info.id left join\n\
        (\n\
            select dic_code,\n\
                   dic_name\n\
            from {data_base}.ods_base_dic_full\n\
            where dt = '{start_date}' and parent_code = '31'\n\
        ) as dic on rule.activity_type = dic.dic_code;"

    osd2dim_sqls = [dim_user_zip, dim_sku_full, dim_province_full, dim_coupon_full, dim_activity_full]
    return osd2dim_sqls
