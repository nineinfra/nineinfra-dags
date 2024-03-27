DATA_BASE = "datahouse"
start_date = "2024-03-27"

dws_trade_user_sku_order_1d=f"\
    set hive.exec.dynamic.partition.mode=nonstrict;\
    insert overwrite table ${DATA_BASE}.dws_trade_user_sku_order_1d partition (dt)\
    select order_detail.user_id,\
           sku.id,\
           sku.sku_name,\
           sku.category1_id,\
           sku.category1_name,\
           sku.category2_id,\
           sku.category2_name,\
           sku.category3_id,\
           sku.category3_name,\
           sku.tm_id,\
           sku.tm_name,\
           order_detail.order_count_1d,\
           order_detail.order_num_1d,\
           order_detail.order_original_amount_1d,\
           order_detail.activity_reduce_amount_1d,\
           order_detail.coupon_reduce_amount_1d,\
           order_detail.order_total_amount_1d,\
           order_detail.dt\
    from\
    (\
        select dt,\
               user_id,\
               sku_id,\
               count(*)                             as order_count_1d,\
               sum(sku_num)                         as order_num_1d,\
               sum(split_original_amount)           as order_original_amount_1d,\
               sum(nvl(split_activity_amount, 0.0)) as activity_reduce_amount_1d,\
               sum(nvl(split_coupon_amount,   0.0)) as coupon_reduce_amount_1d,\
               sum(split_total_amount)              as order_total_amount_1d\
        from ${DATA_BASE}.dwd_trade_order_detail_inc\
        group by dt, user_id, sku_id\
    ) as order_detail left join\
    (\
        select id,\
               sku_name,\
               category1_id,\
               category1_name,\
               category2_id,\
               category2_name,\
               category3_id,\
               category3_name,\
               tm_id,\
               tm_name\
       from ${DATA_BASE}.dim_sku_full\
       where dt = '${start_date}'\
    ) as sku on order_detail.sku_id = sku.id;\
    set hive.exec.dynamic.partition.mode=strict;"


dws_trade_user_sku_order_refund_1d=f"\
    set hive.exec.dynamic.partition.mode=nonstrict;\
    insert overwrite table ${DATA_BASE}.dws_trade_user_sku_order_refund_1d partition (dt)\
    select order_refund.user_id,\
           order_refund.sku_id,\
           sku.sku_name,\
           sku.category1_id,\
           sku.category1_name,\
           sku.category2_id,\
           sku.category2_name,\
           sku.category3_id,\
           sku.category3_name,\
           sku.tm_id,\
           sku.tm_name,\
           order_refund.order_refund_count,\
           order_refund.order_refund_num,\
           order_refund.order_refund_amount,\
           order_refund.dt\
    from\
    (\
        select dt,\
               user_id,\
               sku_id,\
               count(*)           as order_refund_count,\
               sum(refund_num)    as order_refund_num,\
               sum(refund_amount) as order_refund_amount\
        from ${DATA_BASE}.dwd_trade_order_refund_inc\
        group by dt, user_id, sku_id\
    ) as order_refund left join\
    (\
        select id,\
               sku_name,\
               category1_id,\
               category1_name,\
               category2_id,\
               category2_name,\
               category3_id,\
               category3_name,\
               tm_id,\
               tm_name\
        from ${DATA_BASE}.dim_sku_full\
        where dt = '${start_date}'\
    ) as sku on order_refund.sku_id = sku.id;\
    set hive.exec.dynamic.partition.mode=strict;"


dws_trade_user_order_1d=f"\
    insert overwrite table ${DATA_BASE}.dws_trade_user_order_1d partition (dt)\
    select user_id,\
           count(distinct (order_id))         as order_count_1d,\
           sum(sku_num)                       as order_num_1d,\
           sum(split_original_amount)         as order_original_amount_1d,\
           sum(nvl(split_activity_amount, 0)) as activity_reduce_amount_1d,\
           sum(nvl(split_coupon_amount,   0)) as coupon_reduce_amount_1d,\
           sum(split_total_amount)            as order_total_amount_1d,\
           dt\
    from ${DATA_BASE}.dwd_trade_order_detail_inc\
    group by user_id, dt;"


dws_trade_user_cart_add_1d=f"\
    insert overwrite table ${DATA_BASE}.dws_trade_user_cart_add_1d partition (dt)\
    select user_id,\
           count(*)      as cart_add_count_1d,\
           sum(sku_num)  as cart_add_num_1d,\
           dt\
    from ${DATA_BASE}.dwd_trade_cart_add_inc\
    group by user_id, dt;"


dws_trade_user_payment_1d=f"\
    insert overwrite table ${DATA_BASE}.dws_trade_user_payment_1d partition (dt)\
    select user_id,\
           count(distinct (order_id)) as payment_count_1d,\
           sum(sku_num)               as payment_num_1d,\
           sum(split_payment_amount)  as payment_amount_1d,\
           dt\
    from ${DATA_BASE}.dwd_trade_pay_detail_suc_inc\
    group by user_id, dt;"


dws_trade_user_order_refund_1d=f"\
    set hive.exec.dynamic.partition.mode=nonstrict;\
    insert overwrite table ${DATA_BASE}.dws_trade_user_order_refund_1d partition (dt)\
    select user_id,\
           count(*)           as order_refund_count,\
           sum(refund_num)    as order_refund_num,\
           sum(refund_amount) as order_refund_amount,\
           dt\
    from ${DATA_BASE}.dwd_trade_order_refund_inc\
    group by user_id, dt;"


dws_trade_province_order_1d=f"\
    set hive.exec.dynamic.partition.mode=nonstrict;\
    insert overwrite table ${DATA_BASE}.dws_trade_province_order_1d partition (dt)\
    select province.id                             as province_id,\
           province.province_name,\
           province.area_code,\
           province.iso_code,\
           province.iso_3166_2,\
           order_detail.order_count_1d,\
           order_detail.order_original_amount_1d,\
           order_detail.activity_reduce_amount_1d,\
           order_detail.coupon_reduce_amount_1d,\
           order_detail.order_total_amount_1d,\
           order_detail.dt\
    from\
    (\
        select province_id,\
               count(distinct (order_id))         as order_count_1d,\
               sum(split_original_amount)         as order_original_amount_1d,\
               sum(nvl(split_activity_amount, 0)) as activity_reduce_amount_1d,\
               sum(nvl(split_coupon_amount, 0))   as coupon_reduce_amount_1d,\
               sum(split_total_amount)            as order_total_amount_1d,\
               dt\
        from ${DATA_BASE}.dwd_trade_order_detail_inc\
        group by province_id, dt\
    ) as order_detail left join\
    (\
        select id,\
               province_name,\
               area_code,\
               iso_code,\
               iso_3166_2\
        from ${DATA_BASE}.dim_province_full\
        where dt = '${start_date}'\
    ) as province on order_detail.province_id = province.id;\
    set hive.exec.dynamic.partition.mode=strict;"


dws_traffic_session_page_view_1d=f"\
    insert overwrite table ${DATA_BASE}.dws_traffic_session_page_view_1d partition (dt = '${start_date}')\
    select session_id,\
           mid_id,\
           brand,\
           model,\
           operate_system,\
           version_code,\
           channel,\
           sum(during_time) as during_time_1d,\
           count(*)         as page_count_1d\
    from ${DATA_BASE}.dwd_traffic_page_view_inc\
    where dt = '${start_date}'\
    group by session_id, mid_id, brand, model, operate_system, version_code, channel;"


dws_traffic_page_visitor_page_view_1d=f"\
    insert overwrite table ${DATA_BASE}.dws_traffic_page_visitor_page_view_1d partition (dt = '${start_date}')\
    select mid_id,\
           brand,\
           model,\
           operate_system,\
           page_id,\
           sum(during_time) as during_time_1d,\
           count(*)         as view_count_1d\
    from ${DATA_BASE}.dwd_traffic_page_view_inc\
    where dt = '${start_date}'\
    group by mid_id, brand, model, operate_system, page_id;"


dws_trade_user_sku_order_nd=f"\
    insert overwrite table ${DATA_BASE}.dws_trade_user_sku_order_nd partition (dt = '${start_date}')\
    select user_id,\
           sku_id,\
           sku_name,\
           category1_id,\
           category1_name,\
           category2_id,\
           category2_name,\
           category3_id,\
           category3_name,\
           tm_id,\
           tm_name,\
           sum(if(dt >= date_add('${start_date}', -6), order_count_1d,            0)) as order_count_7d,\
           sum(if(dt >= date_add('${start_date}', -6), order_num_1d,              0)) as order_num_7d,\
           sum(if(dt >= date_add('${start_date}', -6), order_original_amount_1d,  0)) as order_original_amount_7d,\
           sum(if(dt >= date_add('${start_date}', -6), activity_reduce_amount_1d, 0)) as activity_reduce_amount_7d,\
           sum(if(dt >= date_add('${start_date}', -6), coupon_reduce_amount_1d,   0)) as coupon_reduce_amount_7d,\
           sum(if(dt >= date_add('${start_date}', -6), order_total_amount_1d,     0)) as order_total_amount_7d,\
           sum(order_count_1d)                                                     as order_count_30d,\
           sum(order_num_1d)                                                       as order_num_30d,\
           sum(order_original_amount_1d)                                           as order_original_amount_30d,\
           sum(activity_reduce_amount_1d)                                          as activity_reduce_amount_30d,\
           sum(coupon_reduce_amount_1d)                                            as coupon_reduce_amount_30d,\
           sum(order_total_amount_1d)                                              as order_total_amount_30d\
    from ${DATA_BASE}.dws_trade_user_sku_order_1d\
    where dt >= date_add('${start_date}', -29) and dt <= '${start_date}'\
    group by user_id,      sku_id,         sku_name,     category1_id,   category1_name,\
             category2_id, category2_name, category3_id, category3_name, tm_id, tm_name;"


dws_trade_user_sku_order_refund_nd=f"\
    insert overwrite table ${DATA_BASE}.dws_trade_user_sku_order_refund_nd partition (dt = '${start_date}')\
    select user_id,\
           sku_id,\
           sku_name,\
           category1_id,\
           category1_name,\
           category2_id,\
           category2_name,\
           category3_id,\
           category3_name,\
           tm_id,\
           tm_name,\
           sum(if(dt >= date_add('${start_date}', -6), order_refund_count_1d,  0)) as order_refund_count_7d,\
           sum(if(dt >= date_add('${start_date}', -6), order_refund_num_1d,    0)) as order_refund_num_7d,\
           sum(if(dt >= date_add('${start_date}', -6), order_refund_amount_1d, 0)) as order_refund_amount_7d,\
           sum(order_refund_count_1d)                                           as order_refund_count_30d,\
           sum(order_refund_num_1d)                                             as order_refund_num_30d,\
           sum(order_refund_amount_1d)                                          as order_refund_amount_30d\
    from ${DATA_BASE}.dws_trade_user_sku_order_refund_1d\
    where dt >= date_add('${start_date}', -29) and dt <= '${start_date}'\
    group by user_id,      sku_id,         sku_name,     category1_id,   category1_name,\
             category2_id, category2_name, category3_id, category3_name, tm_id, tm_name;"


dws_trade_user_order_nd=f"\
    insert overwrite table ${DATA_BASE}.dws_trade_user_order_nd partition (dt = '${start_date}')\
        select user_id,\
               sum(if(dt >= date_add('${start_date}', -6), order_count_1d,            0)) as order_count_7d,\
               sum(if(dt >= date_add('${start_date}', -6), order_num_1d,              0)) as order_num_7d,\
               sum(if(dt >= date_add('${start_date}', -6), order_original_amount_1d,  0)) as order_original_amount_7d,\
               sum(if(dt >= date_add('${start_date}', -6), activity_reduce_amount_1d, 0)) as activity_reduce_amount_7d,\
               sum(if(dt >= date_add('${start_date}', -6), coupon_reduce_amount_1d,   0)) as coupon_reduce_amount_7d,\
               sum(if(dt >= date_add('${start_date}', -6), order_total_amount_1d,     0)) as order_total_amount_7d,\
               sum(order_count_1d)                                                     as order_count_30d,\
               sum(order_num_1d)                                                       as order_num_30d,\
               sum(order_original_amount_1d)                                           as order_original_amount_30d,\
               sum(activity_reduce_amount_1d)                                          as activity_reduce_amount_30d,\
               sum(coupon_reduce_amount_1d)                                            as coupon_reduce_amount_30d,\
               sum(order_total_amount_1d)                                              as order_total_amount_30d\
    from ${DATA_BASE}.dws_trade_user_order_1d\
    where dt >= date_add('${start_date}', -29) and dt <= '${start_date}'\
    group by user_id;"


dws_trade_user_cart_add_nd=f"\
    insert overwrite table ${DATA_BASE}.dws_trade_user_cart_add_nd partition (dt = '${start_date}')\
    select user_id,\
           sum(if(dt >= date_add('${start_date}', -6), cart_add_count_1d, 0)) as cart_add_count_7d,\
           sum(if(dt >= date_add('${start_date}', -6), cart_add_num_1d,   0)) as cart_add_num_7d,\
           sum(cart_add_count_1d)                                          as cart_add_count_30d,\
           sum(cart_add_num_1d)                                            as cart_add_num_30d\
    from ${DATA_BASE}.dws_trade_user_cart_add_1d\
    where dt >= date_add('${start_date}', -29) and dt <= '${start_date}'\
    group by user_id;"


dws_trade_user_payment_nd=f"\
    insert overwrite table ${DATA_BASE}.dws_trade_user_payment_nd partition (dt = '${start_date}')\
    select user_id,\
           sum(if(dt >= date_add('${start_date}', -6), payment_count_1d,  0)) as payment_count_7d,\
           sum(if(dt >= date_add('${start_date}', -6), payment_num_1d,    0)) as payment_num_7d,\
           sum(if(dt >= date_add('${start_date}', -6), payment_amount_1d, 0)) as payment_amount_7d,\
           sum(payment_count_1d)                                           as payment_count_30d,\
           sum(payment_num_1d)                                             as payment_num_30d,\
           sum(payment_amount_1d)                                          as payment_amount_30d\
    from ${DATA_BASE}.dws_trade_user_payment_1d\
    where dt >= date_add('${start_date}', -29) and dt <= '${start_date}'\
    group by user_id;"


dws_trade_user_order_refund_nd=f"\
    insert overwrite table ${DATA_BASE}.dws_trade_user_order_refund_nd partition (dt = '${start_date}')\
    select user_id,\
           sum(if(dt >= date_add('${start_date}', -6), order_refund_count_1d,  0)) as order_refund_count_7d,\
           sum(if(dt >= date_add('${start_date}', -6), order_refund_num_1d,    0)) as order_refund_num_7d,\
           sum(if(dt >= date_add('${start_date}', -6), order_refund_amount_1d, 0)) as order_refund_amount_7d,\
           sum(order_refund_count_1d)                                           as order_refund_count_30d,\
           sum(order_refund_num_1d)                                             as order_refund_num_30d,\
           sum(order_refund_amount_1d)                                          as order_refund_amount_30d\
    from ${DATA_BASE}.dws_trade_user_order_refund_1d\
    where dt >= date_add('${start_date}', -29) and dt <= '${start_date}'\
    group by user_id;"


dws_trade_province_order_nd=f"\
    insert overwrite table ${DATA_BASE}.dws_trade_province_order_nd partition (dt = '${start_date}')\
    select province_id,\
           province_name,\
           area_code,\
           iso_code,\
           iso_3166_2,\
           sum(if(dt >= date_add('${start_date}', -6), order_count_1d,            0)) as order_count_7d,\
           sum(if(dt >= date_add('${start_date}', -6), order_original_amount_1d,  0)) as order_original_amount_7d,\
           sum(if(dt >= date_add('${start_date}', -6), activity_reduce_amount_1d, 0)) as activity_reduce_amount_7d,\
           sum(if(dt >= date_add('${start_date}', -6), coupon_reduce_amount_1d,   0)) as coupon_reduce_amount_7d,\
           sum(if(dt >= date_add('${start_date}', -6), order_total_amount_1d,     0)) as order_total_amount_7d,\
           sum(order_count_1d)                                                     as order_count_30d,\
           sum(order_original_amount_1d)                                           as order_original_amount_30d,\
           sum(activity_reduce_amount_1d)                                          as activity_reduce_amount_30,\
           sum(coupon_reduce_amount_1d)                                            as coupon_reduce_amount_30d,\
           sum(order_total_amount_1d)                                              as order_total_amount_30d\
    from ${DATA_BASE}.dws_trade_province_order_1d\
    where dt >= date_add('${start_date}', -29) and dt <= '${start_date}'\
    group by province_id, province_name, area_code, iso_code, iso_3166_2;"


dws_trade_coupon_order_nd=f"\
    insert overwrite table ${DATA_BASE}.dws_trade_coupon_order_nd partition (dt = '${start_date}')\
    select coupon.id,\
           coupon.coupon_name,\
           coupon.coupon_type_code,\
           coupon.coupon_type_name,\
           coupon.benefit_rule,\
           coupon.start_date,\
           sum(order_detail.split_original_amount) as original_amount_30d,\
           sum(order_detail.split_coupon_amount)   as coupon_reduce_amount_30d\
    from\
    (\
        select id,\
               coupon_name,\
               coupon_type_code,\
               coupon_type_name,\
               benefit_rule,\
               date_format(start_time, 'yyyy-MM-dd') as start_date\
        from ${DATA_BASE}.dim_coupon_full\
        where dt = '${start_date}' and date_format(start_time, 'yyyy-MM-dd') >= date_add('${start_date}', -29)\
    ) as coupon left join\
    (\
        select coupon_id,\
               order_id,\
               split_original_amount,\
               split_coupon_amount\
        from ${DATA_BASE}.dwd_trade_order_detail_inc\
        where dt >= date_add('${start_date}', -29) and dt <= '${start_date}' and coupon_id is not null\
    ) as order_detail\
        on coupon.id = order_detail.coupon_id\
    group by coupon.id,               coupon.coupon_name,  coupon.coupon_type_code,\
             coupon.coupon_type_name, coupon.benefit_rule, coupon.start_date;"


dws_trade_activity_order_nd=f"\
    insert overwrite table ${DATA_BASE}.dws_trade_activity_order_nd partition (dt = '${start_date}')\
    select activity.activity_id,\
           activity.activity_name,\
           activity.activity_type_code,\
           activity.activity_type_name,\
           date_format(activity.start_time, 'yyyy-MM-dd') as start_date,\
           sum(order_detail.split_original_amount)        as original_amount_30d,\
           sum(order_detail.split_activity_amount)        as activity_reduce_amount_30d\
    from\
    (\
        select activity_id,\
               activity_name,\
               activity_type_code,\
               activity_type_name,\
               start_time\
        from ${DATA_BASE}.dim_activity_full\
        where dt = '${start_date}' and date_format(start_time, 'yyyy-MM-dd') >= date_add('${start_date}', -29)\
        group by activity_id, activity_name, activity_type_code, activity_type_name, start_time\
    ) as activity left join\
    (\
        select activity_id,\
               order_id,\
               split_original_amount,\
               split_activity_amount\
        from ${DATA_BASE}.dwd_trade_order_detail_inc\
        where dt >= date_add('${start_date}', -29) and dt <= '${start_date}' and activity_id is not null\
    ) as order_detail on activity.activity_id = order_detail.activity_id\
    group by activity.activity_id,        activity.activity_name, activity.activity_type_code,\
             activity.activity_type_name, activity.start_time;"


dws_traffic_page_visitor_page_view_nd=f"\
    insert overwrite table ${DATA_BASE}.dws_traffic_page_visitor_page_view_nd partition (dt = '${start_date}')\
    select mid_id,\
           brand,\
           model,\
           operate_system,\
           page_id,\
           sum(if(dt >= date_add('${start_date}', -6), during_time_1d, 0)) as during_time_7d,\
           sum(if(dt >= date_add('${start_date}', -6), view_count_1d,  0)) as view_count_7d,\
           sum(during_time_1d)                                          as during_time_30d,\
           sum(view_count_1d)                                           as view_count_30d\
    from ${DATA_BASE}.dws_traffic_page_visitor_page_view_1d\
    where dt >= date_add('${start_date}', -29) and dt <= '${start_date}'\
    group by mid_id, brand, model, operate_system, page_id;"


dws_trade_user_order_td=f"\
    insert overwrite table ${DATA_BASE}.dws_trade_user_order_td partition (dt = '${start_date}')\
    select user_id,\
           min(dt)                        as order_date_first,\
           max(dt)                        as order_date_last,\
           sum(order_count_1d)            as order_count_td,\
           sum(order_num_1d)              as order_num_td,\
           sum(order_original_amount_1d)  as original_amount_td,\
           sum(activity_reduce_amount_1d) as activity_reduce_amount_td,\
           sum(coupon_reduce_amount_1d)   as coupon_reduce_amount_td,\
           sum(order_total_amount_1d)     as total_amount_td\
    from ${DATA_BASE}.dws_trade_user_order_1d\
    group by user_id;"


dws_trade_user_payment_td=f"\
    insert overwrite table ${DATA_BASE}.dws_trade_user_payment_td partition (dt = '${start_date}')\
    select user_id,\
           min(dt)                as payment_date_first,\
           max(dt)                as payment_date_last,\
           sum(payment_count_1d)  as payment_count_td,\
           sum(payment_num_1d)    as payment_num_td,\
           sum(payment_amount_1d) as payment_amount_td\
    from ${DATA_BASE}.dws_trade_user_payment_1d\
    group by user_id;"


dws_user_user_login_td=f"\
    insert overwrite table ${DATA_BASE}.dws_user_user_login_td partition (dt = '${start_date}')\
    select user_.id                                                                 as user_id,\
           nvl(login.login_date_last, date_format(user_.create_time, 'yyyy-MM-dd')) as login_date_last,\
           nvl(login.login_count_td,  1)                                            as login_count_td\
    from\
    (\
        select id,\
               create_time\
        from ${DATA_BASE}.dim_user_zip\
        where dt = '9999-12-31'\
    ) as user_ left join\
    (\
        select user_id,\
               max(dt) login_date_last,\
               count(*) login_count_td\
        from ${DATA_BASE}.dwd_user_login_inc\
        group by user_id\
    ) as login on user_.id = login.user_id;"