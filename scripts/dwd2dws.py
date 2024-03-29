def get_dwd2dws_sqls(data_base, start_date):
    dws_trade_user_sku_order_1d = f"\n\
        insert overwrite table {data_base}.dws_trade_user_sku_order_1d partition (dt = '{start_date}')\n\
        select order_detail.user_id,\n\
               sku.id,\n\
               sku.sku_name,\n\
               sku.category1_id,\n\
               sku.category1_name,\n\
               sku.category2_id,\n\
               sku.category2_name,\n\
               sku.category3_id,\n\
               sku.category3_name,\n\
               sku.tm_id,\n\
               sku.tm_name,\n\
               order_detail.order_count_1d,\n\
               order_detail.order_num_1d,\n\
               order_detail.order_original_amount_1d,\n\
               order_detail.activity_reduce_amount_1d,\n\
               order_detail.coupon_reduce_amount_1d,\n\
               order_detail.order_total_amount_1d\n\
        from\n\
        (\n\
            select dt,\n\
                   user_id,\n\
                   sku_id,\n\
                   count(*)                             as order_count_1d,\n\
                   sum(sku_num)                         as order_num_1d,\n\
                   sum(split_original_amount)           as order_original_amount_1d,\n\
                   sum(nvl(split_activity_amount, 0.0)) as activity_reduce_amount_1d,\n\
                   sum(nvl(split_coupon_amount,   0.0)) as coupon_reduce_amount_1d,\n\
                   sum(split_total_amount)              as order_total_amount_1d\n\
            from {data_base}.dwd_trade_order_detail_inc\n\
            where dt='{start_date}'\n\
            group by user_id, sku_id\n\
        ) as order_detail left join\n\
        (\n\
            select id,\n\
                   sku_name,\n\
                   category1_id,\n\
                   category1_name,\n\
                   category2_id,\n\
                   category2_name,\n\
                   category3_id,\n\
                   category3_name,\n\
                   tm_id,\n\
                   tm_name\n\
           from {data_base}.dim_sku_full\n\
           where dt = '{start_date}'\n\
        ) as sku on order_detail.sku_id = sku.id;"

    dws_trade_user_sku_order_refund_1d = f"\n\
        insert overwrite table {data_base}.dws_trade_user_sku_order_refund_1d partition (dt = '{start_date}')\n\
        select order_refund.user_id,\n\
               order_refund.sku_id,\n\
               sku.sku_name,\n\
               sku.category1_id,\n\
               sku.category1_name,\n\
               sku.category2_id,\n\
               sku.category2_name,\n\
               sku.category3_id,\n\
               sku.category3_name,\n\
               sku.tm_id,\n\
               sku.tm_name,\n\
               order_refund.order_refund_count,\n\
               order_refund.order_refund_num,\n\
               order_refund.order_refund_amount\n\
        from\n\
        (\n\
            select dt,\n\
                   user_id,\n\
                   sku_id,\n\
                   count(*)           as order_refund_count,\n\
                   sum(refund_num)    as order_refund_num,\n\
                   sum(refund_amount) as order_refund_amount\n\
            from {data_base}.dwd_trade_order_refund_inc\n\
            where dt='{start_date}'\n\
            group by user_id, sku_id\n\
        ) as order_refund left join\n\
        (\n\
            select id,\n\
                   sku_name,\n\
                   category1_id,\n\
                   category1_name,\n\
                   category2_id,\n\
                   category2_name,\n\
                   category3_id,\n\
                   category3_name,\n\
                   tm_id,\n\
                   tm_name\n\
            from {data_base}.dim_sku_full\n\
            where dt = '{start_date}'\n\
        ) as sku on order_refund.sku_id = sku.id;"

    dws_trade_user_order_1d = f"\n\
        insert overwrite table {data_base}.dws_trade_user_order_1d partition (dt = '{start_date}')\n\
        select user_id,\n\
               count(distinct (order_id))         as order_count_1d,\n\
               sum(sku_num)                       as order_num_1d,\n\
               sum(split_original_amount)         as order_original_amount_1d,\n\
               sum(nvl(split_activity_amount, 0)) as activity_reduce_amount_1d,\n\
               sum(nvl(split_coupon_amount,   0)) as coupon_reduce_amount_1d,\n\
               sum(split_total_amount)            as order_total_amount_1d\n\
        from {data_base}.dwd_trade_order_detail_inc\n\
        where dt = '{start_date}'\n\
        group by user_id;"

    dws_trade_user_cart_add_1d = f"\n\
        insert overwrite table {data_base}.dws_trade_user_cart_add_1d partition (dt = '{start_date}')\n\
        select user_id,\n\
               count(*)      as cart_add_count_1d,\n\
               sum(sku_num)  as cart_add_num_1d\n\
        from {data_base}.dwd_trade_cart_add_inc\n\
        where dt = '{start_date}'\n\
        group by user_id;"

    dws_trade_user_payment_1d = f"\n\
        insert overwrite table {data_base}.dws_trade_user_payment_1d partition (dt = '{start_date}')\n\
        select user_id,\n\
               count(distinct (order_id)) as payment_count_1d,\n\
               sum(sku_num)               as payment_num_1d,\n\
               sum(split_payment_amount)  as payment_amount_1d\n\
        from {data_base}.dwd_trade_pay_detail_suc_inc\n\
        where dt = '{start_date}'\n\
        group by user_id;"

    dws_trade_user_order_refund_1d = f"\n\
        insert overwrite table {data_base}.dws_trade_user_order_refund_1d partition (dt = '{start_date}')\n\
        select user_id,\n\
               count(*)           as order_refund_count,\n\
               sum(refund_num)    as order_refund_num,\n\
               sum(refund_amount) as order_refund_amount\n\
        from {data_base}.dwd_trade_order_refund_inc\n\
        where dt = '{start_date}'\n\
        group by user_id;"

    dws_trade_province_order_1d = f"\n\
        insert overwrite table {data_base}.dws_trade_province_order_1d partition (dt = '{start_date}')\n\
        select province.id                             as province_id,\n\
               province.province_name,\n\
               province.area_code,\n\
               province.iso_code,\n\
               province.iso_3166_2,\n\
               order_detail.order_count_1d,\n\
               order_detail.order_original_amount_1d,\n\
               order_detail.activity_reduce_amount_1d,\n\
               order_detail.coupon_reduce_amount_1d,\n\
               order_detail.order_total_amount_1d\n\
        from\n\
        (\n\
            select province_id,\n\
                   count(distinct (order_id))         as order_count_1d,\n\
                   sum(split_original_amount)         as order_original_amount_1d,\n\
                   sum(nvl(split_activity_amount, 0)) as activity_reduce_amount_1d,\n\
                   sum(nvl(split_coupon_amount, 0))   as coupon_reduce_amount_1d,\n\
                   sum(split_total_amount)            as order_total_amount_1d,\n\
                   dt\n\
            from {data_base}.dwd_trade_order_detail_inc\n\
            where dt = '{start_date}'\n\
            group by province_id\n\
        ) as order_detail left join\n\
        (\n\
            select id,\n\
                   province_name,\n\
                   area_code,\n\
                   iso_code,\n\
                   iso_3166_2\n\
            from {data_base}.dim_province_full\n\
            where dt = '{start_date}'\n\
        ) as province on order_detail.province_id = province.id;"

    dws_traffic_session_page_view_1d = f"\n\
        insert overwrite table {data_base}.dws_traffic_session_page_view_1d partition (dt = '{start_date}')\n\
        select session_id,\n\
               mid_id,\n\
               brand,\n\
               model,\n\
               operate_system,\n\
               version_code,\n\
               channel,\n\
               sum(during_time) as during_time_1d,\n\
               count(*)         as page_count_1d\n\
        from {data_base}.dwd_traffic_page_view_inc\n\
        where dt = '{start_date}'\n\
        group by session_id, mid_id, brand, model, operate_system, version_code, channel;"

    dws_traffic_page_visitor_page_view_1d = f"\n\
        insert overwrite table {data_base}.dws_traffic_page_visitor_page_view_1d partition (dt = '{start_date}')\n\
        select mid_id,\n\
               brand,\n\
               model,\n\
               operate_system,\n\
               page_id,\n\
               sum(during_time) as during_time_1d,\n\
               count(*)         as view_count_1d\n\
        from {data_base}.dwd_traffic_page_view_inc\n\
        where dt = '{start_date}'\n\
        group by mid_id, brand, model, operate_system, page_id;"

    dws_trade_user_sku_order_nd = f"\n\
        insert overwrite table {data_base}.dws_trade_user_sku_order_nd partition (dt = '{start_date}')\n\
        select user_id,\n\
               sku_id,\n\
               sku_name,\n\
               category1_id,\n\
               category1_name,\n\
               category2_id,\n\
               category2_name,\n\
               category3_id,\n\
               category3_name,\n\
               tm_id,\n\
               tm_name,\n\
               sum(if(dt >= date_add('{start_date}', -6), order_count_1d,            0)) as order_count_7d,\n\
               sum(if(dt >= date_add('{start_date}', -6), order_num_1d,              0)) as order_num_7d,\n\
               sum(if(dt >= date_add('{start_date}', -6), order_original_amount_1d,  0)) as order_original_amount_7d,\n\
               sum(if(dt >= date_add('{start_date}', -6), activity_reduce_amount_1d, 0)) as activity_reduce_amount_7d,\n\
               sum(if(dt >= date_add('{start_date}', -6), coupon_reduce_amount_1d,   0)) as coupon_reduce_amount_7d,\n\
               sum(if(dt >= date_add('{start_date}', -6), order_total_amount_1d,     0)) as order_total_amount_7d,\n\
               sum(order_count_1d)                                                     as order_count_30d,\n\
               sum(order_num_1d)                                                       as order_num_30d,\n\
               sum(order_original_amount_1d)                                           as order_original_amount_30d,\n\
               sum(activity_reduce_amount_1d)                                          as activity_reduce_amount_30d,\n\
               sum(coupon_reduce_amount_1d)                                            as coupon_reduce_amount_30d,\n\
               sum(order_total_amount_1d)                                              as order_total_amount_30d\n\
        from {data_base}.dws_trade_user_sku_order_1d\n\
        where dt >= date_add('{start_date}', -29) and dt <= '{start_date}'\n\
        group by user_id,      sku_id,         sku_name,     category1_id,   category1_name,\n\
                category2_id, category2_name, category3_id, category3_name, tm_id, tm_name;"

    dws_trade_user_sku_order_refund_nd = f"\n\
        insert overwrite table {data_base}.dws_trade_user_sku_order_refund_nd partition (dt = '{start_date}')\n\
        select user_id,\n\
               sku_id,\n\
               sku_name,\n\
               category1_id,\n\
               category1_name,\n\
               category2_id,\n\
               category2_name,\n\
               category3_id,\n\
               category3_name,\n\
               tm_id,\n\
               tm_name,\n\
               sum(if(dt >= date_add('{start_date}', -6), order_refund_count_1d,  0)) as order_refund_count_7d,\n\
               sum(if(dt >= date_add('{start_date}', -6), order_refund_num_1d,    0)) as order_refund_num_7d,\n\
               sum(if(dt >= date_add('{start_date}', -6), order_refund_amount_1d, 0)) as order_refund_amount_7d,\n\
               sum(order_refund_count_1d)                                           as order_refund_count_30d,\n\
               sum(order_refund_num_1d)                                             as order_refund_num_30d,\n\
               sum(order_refund_amount_1d)                                          as order_refund_amount_30d\n\
         from {data_base} dws_trade_user_sku_order_refund_1d\n\
        where dt >= date_add('{start_date}', -29) and dt <= '{start_date}'\n\
        group by user_id,      sku_id,         sku_name,     category1_id,   category1_name,\n\
            category2_id, category2_name, category3_id, category3_name, tm_id, tm_name;"

    dws_trade_user_order_nd = f"\n\
        insert overwrite table {data_base}.dws_trade_user_order_nd partition (dt = '{start_date}')\n\
            select user_id,\n\
                   sum(if(dt >= date_add('{start_date}', -6), order_count_1d,            0)) as order_count_7d,\n\
                   sum(if(dt >= date_add('{start_date}', -6), order_num_1d,              0)) as order_num_7d,\n\
                   sum(if(dt >= date_add('{start_date}', -6), order_original_amount_1d,  0)) as order_original_amount_7d,\n\
                   sum(if(dt >= date_add('{start_date}', -6), activity_reduce_amount_1d, 0)) as activity_reduce_amount_7d,\n\
                   sum(if(dt >= date_add('{start_date}', -6), coupon_reduce_amount_1d,   0)) as coupon_reduce_amount_7d,\n\
                   sum(if(dt >= date_add('{start_date}', -6), order_total_amount_1d,     0)) as order_total_amount_7d,\n\
                   sum(order_count_1d)                                                     as order_count_30d,\n\
                   sum(order_num_1d)                                                       as order_num_30d,\n\
                   sum(order_original_amount_1d)                                           as order_original_amount_30d,\n\
                   sum(activity_reduce_amount_1d)                                          as activity_reduce_amount_30d,\n\
                   sum(coupon_reduce_amount_1d)                                            as coupon_reduce_amount_30d,\n\
                   sum(order_total_amount_1d)                                              as order_total_amount_30d\n\
         from {data_base} dws_trade_user_order_1d\n\
        where dt >= date_add('{start_date}', -29) and dt <= '{start_date}'\n\
        group by user_id;"

    dws_trade_user_cart_add_nd = f"\n\
        insert overwrite table {data_base}.dws_trade_user_cart_add_nd partition (dt = '{start_date}')\n\
        select user_id,\n\
               sum(if(dt >= date_add('{start_date}', -6), cart_add_count_1d, 0)) as cart_add_count_7d,\n\
               sum(if(dt >= date_add('{start_date}', -6), cart_add_num_1d,   0)) as cart_add_num_7d,\n\
               sum(cart_add_count_1d)                                          as cart_add_count_30d,\n\
               sum(cart_add_num_1d)                                            as cart_add_num_30d\n\
         from {data_base} dws_trade_user_cart_add_1d\n\
        where dt >= date_add('{start_date}', -29) and dt <= '{start_date}'\n\
        group by user_id;"

    dws_trade_user_payment_nd = f"\n\
        insert overwrite table {data_base}.dws_trade_user_payment_nd partition (dt = '{start_date}')\n\
        select user_id,\n\
               sum(if(dt >= date_add('{start_date}', -6), payment_count_1d,  0)) as payment_count_7d,\n\
               sum(if(dt >= date_add('{start_date}', -6), payment_num_1d,    0)) as payment_num_7d,\n\
               sum(if(dt >= date_add('{start_date}', -6), payment_amount_1d, 0)) as payment_amount_7d,\n\
               sum(payment_count_1d)                                           as payment_count_30d,\n\
               sum(payment_num_1d)                                             as payment_num_30d,\n\
               sum(payment_amount_1d)                                          as payment_amount_30d\n\
         from {data_base} dws_trade_user_payment_1d\n\
        where dt >= date_add('{start_date}', -29) and dt <= '{start_date}'\n\
        group by user_id;"

    dws_trade_user_order_refund_nd = f"\n\
        insert overwrite table {data_base}.dws_trade_user_order_refund_nd partition (dt = '{start_date}')\n\
        select user_id,\n\
               sum(if(dt >= date_add('{start_date}', -6), order_refund_count_1d,  0)) as order_refund_count_7d,\n\
               sum(if(dt >= date_add('{start_date}', -6), order_refund_num_1d,    0)) as order_refund_num_7d,\n\
               sum(if(dt >= date_add('{start_date}', -6), order_refund_amount_1d, 0)) as order_refund_amount_7d,\n\
               sum(order_refund_count_1d)                                           as order_refund_count_30d,\n\
               sum(order_refund_num_1d)                                             as order_refund_num_30d,\n\
               sum(order_refund_amount_1d)                                          as order_refund_amount_30d\n\
         from {data_base} dws_trade_user_order_refund_1d\n\
        where dt >= date_add('{start_date}', -29) and dt <= '{start_date}'\n\
        group by user_id;"

    dws_trade_province_order_nd = f"\n\
        insert overwrite table {data_base}.dws_trade_province_order_nd partition (dt = '{start_date}')\n\
        select province_id,\n\
               province_name,\n\
               area_code,\n\
               iso_code,\n\
               iso_3166_2,\n\
               sum(if(dt >= date_add('{start_date}', -6), order_count_1d,            0)) as order_count_7d,\n\
               sum(if(dt >= date_add('{start_date}', -6), order_original_amount_1d,  0)) as order_original_amount_7d,\n\
               sum(if(dt >= date_add('{start_date}', -6), activity_reduce_amount_1d, 0)) as activity_reduce_amount_7d,\n\
               sum(if(dt >= date_add('{start_date}', -6), coupon_reduce_amount_1d,   0)) as coupon_reduce_amount_7d,\n\
               sum(if(dt >= date_add('{start_date}', -6), order_total_amount_1d,     0)) as order_total_amount_7d,\n\
               sum(order_count_1d)                                                     as order_count_30d,\n\
               sum(order_original_amount_1d)                                           as order_original_amount_30d,\n\
               sum(activity_reduce_amount_1d)                                          as activity_reduce_amount_30,\n\
               sum(coupon_reduce_amount_1d)                                            as coupon_reduce_amount_30d,\n\
               sum(order_total_amount_1d)                                              as order_total_amount_30d\n\
         from {data_base} dws_trade_province_order_1d\n\
        where dt >= date_add('{start_date}', -29) and dt <= '{start_date}'\n\
        group by province_id, province_name, area_code, iso_code, iso_3166_2;"

    dws_trade_coupon_order_nd = f"\n\
        insert overwrite table {data_base}.dws_trade_coupon_order_nd partition (dt = '{start_date}')\n\
        select coupon.id,\n\
               coupon.coupon_name,\n\
               coupon.coupon_type_code,\n\
               coupon.coupon_type_name,\n\
               coupon.benefit_rule,\n\
               coupon.start_date,\n\
               sum(order_detail.split_original_amount) as original_amount_30d,\n\
               sum(order_detail.split_coupon_amount)   as coupon_reduce_amount_30d\n\
        from\n\
        (\n\
            select id,\n\
                   coupon_name,\n\
                   coupon_type_code,\n\
                   coupon_type_name,\n\
                   benefit_rule,\n\
                   date_format(start_time, 'yyyy-MM-dd') as start_date\n\
            from {data_base}.dim_coupon_full\n\
            where dt = '{start_date}' and date_format(start_time, 'yyyy-MM-dd') >= date_add('{start_date}', -29)\n\
        ) as coupon left join\n\
        (\n\
            select coupon_id,\n\
                   order_id,\n\
                   split_original_amount,\n\
                   split_coupon_amount\n\
            from {data_base}.dwd_trade_order_detail_inc\n\
            where dt >= date_add('{start_date}', -29) and dt <= '{start_date}' and coupon_id is not null\n\
        ) as order_detail\n\
            on coupon.id = order_detail.coupon_id\n\
        group by coupon.id,               coupon.coupon_name,  coupon.coupon_type_code,\n\
                coupon.coupon_type_name, coupon.benefit_rule, coupon.start_date;"

    dws_trade_activity_order_nd = f"\n\
        insert overwrite table {data_base}.dws_trade_activity_order_nd partition (dt = '{start_date}')\n\
        select activity.activity_id,\n\
               activity.activity_name,\n\
               activity.activity_type_code,\n\
               activity.activity_type_name,\n\
               date_format(activity.start_time, 'yyyy-MM-dd') as start_date,\n\
               sum(order_detail.split_original_amount)        as original_amount_30d,\n\
               sum(order_detail.split_activity_amount)        as activity_reduce_amount_30d\n\
        from\n\
        (\n\
            select activity_id,\n\
                   activity_name,\n\
                   activity_type_code,\n\
                   activity_type_name,\n\
                   start_time\n\
            from {data_base}.dim_activity_full\n\
            where dt = '{start_date}' and date_format(start_time, 'yyyy-MM-dd') >= date_add('{start_date}', -29)\n\
            group by activity_id, activity_name, activity_type_code, activity_type_name, start_time\n\
        ) as activity left join\n\
        (\n\
            select activity_id,\n\
                   order_id,\n\
                   split_original_amount,\n\
                   split_activity_amount\n\
            from {data_base}.dwd_trade_order_detail_inc\n\
            where dt >= date_add('{start_date}', -29) and dt <= '{start_date}' and activity_id is not null\n\
        ) as order_detail on activity.activity_id = order_detail.activity_id\n\
        group by activity.activity_id,        activity.activity_name, activity.activity_type_code,\n\
                activity.activity_type_name, activity.start_time;"

    dws_traffic_page_visitor_page_view_nd = f"\n\
        insert overwrite table {data_base}.dws_traffic_page_visitor_page_view_nd partition (dt = '{start_date}')\n\
        select mid_id,\n\
               brand,\n\
               model,\n\
               operate_system,\n\
               page_id,\n\
               sum(if(dt >= date_add('{start_date}', -6), during_time_1d, 0)) as during_time_7d,\n\
               sum(if(dt >= date_add('{start_date}', -6), view_count_1d,  0)) as view_count_7d,\n\
               sum(during_time_1d)                                          as during_time_30d,\n\
               sum(view_count_1d)                                           as view_count_30d\n\
         from {data_base} dws_traffic_page_visitor_page_view_1d\n\
        where dt >= date_add('{start_date}', -29) and dt <= '{start_date}'\n\
        group by mid_id, brand, model, operate_system, page_id;"

    dws_trade_user_order_td = f"\n\
        insert overwrite table {data_base}.dws_trade_user_order_td partition (dt = '{start_date}')\n\
        select nvl(old.user_id, new.user_id)                                                 as user_id,\n\
               if(old.user_id is null,     '{start_date}', old.order_date_first)               as order_date_first,\n\
               if(new.user_id is not null, '{start_date}', old.order_date_last)                as order_date_last,\n\
               nvl(old.order_count_td,            0) + nvl(new.order_count_1d,            0) as order_count_td,\n\
               nvl(old.order_num_td,              0) + nvl(new.order_num_1d,              0) as order_num_td,\n\
               nvl(old.original_amount_td,        0) + nvl(new.order_original_amount_1d,  0) as original_amount_td,\n\
               nvl(old.activity_reduce_amount_td, 0) + nvl(new.activity_reduce_amount_1d, 0) as activity_reduce_amount_td,\n\
               nvl(old.coupon_reduce_amount_td,   0) + nvl(new.coupon_reduce_amount_1d,   0) as coupon_reduce_amount_td,\n\
               nvl(old.total_amount_td,           0) + nvl(new.order_total_amount_1d,     0) as total_amount_td\n\
        from\n\
        (\n\
            select user_id,\n\
                   order_date_first,\n\
                   order_date_last,\n\
                   order_count_td,\n\
                   order_num_td,\n\
                   original_amount_td,\n\
                   activity_reduce_amount_td,\n\
                   coupon_reduce_amount_td,\n\
                   total_amount_td\n\
            from {data_base}.dws_trade_user_order_td\n\
            where dt = date_add('{start_date}', -1)\n\
        ) as old full outer join\n\
        (\n\
            select user_id,\n\
                   order_count_1d,\n\
                   order_num_1d,\n\
                   order_original_amount_1d,\n\
                   activity_reduce_amount_1d,\n\
                   coupon_reduce_amount_1d,\n\
                   order_total_amount_1d\n\
            from {data_base}.dws_trade_user_order_1d\n\
            where dt = '{start_date}'\n\
        ) as new on old.user_id = new.user_id;"

    dws_trade_user_payment_td = f"\n\
        insert overwrite table {data_base}.dws_trade_user_payment_td partition (dt = '{start_date}')\n\
        select nvl(old.user_id, new.user_id)                                     as user_id,\n\
               if(old.user_id is null,     '{start_date}', old.payment_date_first) as payment_date_first,\n\
               if(new.user_id is not null, '{start_date}', old.payment_date_last)  as payment_date_last,\n\
               nvl(old.payment_count_td,  0) + nvl(new.payment_count_1d, 0)      as payment_count_td,\n\
               nvl(old.payment_num_td,    0) + nvl(new.payment_num_1d, 0)        as payment_num_td,\n\
               nvl(old.payment_amount_td, 0) + nvl(new.payment_amount_1d, 0)     as payment_amount_td\n\
        from\n\
        (\n\
            select user_id,\n\
                   payment_date_first,\n\
                   payment_date_last,\n\
                   payment_count_td,\n\
                   payment_num_td,\n\
                   payment_amount_td\n\
            from {data_base}.dws_trade_user_payment_td\n\
            where dt = date_add('{start_date}', -1)\n\
        ) as old full outer join\n\
        (\n\
            select user_id,\n\
                   payment_count_1d,\n\
                   payment_num_1d,\n\
                   payment_amount_1d\n\
           from {data_base}.dws_trade_user_payment_1d\n\
           where dt = '{start_date}'\n\
        ) as new on old.user_id = new.user_id;"

    dws_user_user_login_td = f"\n\
        insert overwrite table {data_base}.dws_user_user_login_td partition (dt = '{start_date}')\n\
        select nvl(old.user_id, new.user_id)                              as user_id,\n\
               if(new.user_id is null, old.login_date_last, '{start_date}') as login_date_last,\n\
               nvl(old.login_count_td, 0) + nvl(new.login_count_1d, 0)    as login_count_td\n\
        from\n\
        (\n\
            select user_id,\n\
                   login_date_last,\n\
                   login_count_td\n\
            from {data_base}.dws_user_user_login_td\n\
            where dt = date_add('{start_date}', -1)\n\
        ) as old full outer join\n\
        (\n\
            select user_id,\n\
                   count(*) as login_count_1d\n\
            from {data_base}.dwd_user_login_inc\n\
            where dt = '{start_date}'\n\
            group by user_id\n\
        ) as new on old.user_id = new.user_id;"
    dwd2dws_sqls = [dws_trade_user_sku_order_1d, dws_trade_user_sku_order_refund_1d,
                    dws_trade_user_order_1d, dws_trade_user_cart_add_1d, dws_trade_user_payment_1d,
                    dws_trade_user_order_refund_1d, dws_trade_province_order_1d,
                    dws_traffic_session_page_view_1d, dws_traffic_page_visitor_page_view_1d,
                    dws_trade_user_sku_order_nd, dws_trade_user_sku_order_refund_nd,
                    dws_trade_user_order_nd, dws_trade_user_cart_add_nd, dws_trade_user_payment_nd,
                    dws_trade_user_order_refund_nd, dws_trade_province_order_nd,
                    dws_trade_coupon_order_nd, dws_trade_activity_order_nd,
                    dws_traffic_page_visitor_page_view_nd, dws_trade_user_order_td,
                    dws_trade_user_payment_td, dws_user_user_login_td]
    return dwd2dws_sqls
