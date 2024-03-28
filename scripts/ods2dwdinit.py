DATA_BASE = "datahouse"
start_date = "2024-03-27"

dwd_trade_cart_add_inc = f"\n\
    set hive.exec.dynamic.partition.mode=nonstrict;\n\
    insert overwrite table {DATA_BASE}.dwd_trade_cart_add_inc partition (dt)\n\
    select cart.id,\n\
           cart.user_id,\n\
           cart.sku_id,\n\
           date_format(cart.create_time, 'yyyy-MM-dd') as date_id,\n\
           cart.create_time,\n\
           cart.source_id,\n\
           cart.source_type                            as source_type_code,\n\
           dic.dic_name                                as source_type_name,\n\
           cart.sku_num,\n\
           date_format(cart.create_time, 'yyyy-MM-dd') as dt\n\
    from\n\
    (\n\
        select data.id,\n\
               data.user_id,\n\
               data.sku_id,\n\
               data.create_time,\n\
               data.source_id,\n\
               data.source_type,\n\
               data.sku_num\n\
        from {DATA_BASE}.ods_cart_info_inc\n\
        where dt = '{start_date}' and type = 'bootstrap-insert'\n\
    ) as cart left join\n\
    (\n\
        select dic_code,\n\
               dic_name\n\
        from {DATA_BASE}.ods_base_dic_full\n\
        where dt = '{start_date}' and parent_code = '24'\n\
    ) as dic on cart.source_type = dic.dic_code;\n\
    set hive.exec.dynamic.partition.mode=strict;"

dwd_trade_order_detail_inc = f"\n\
    set hive.exec.dynamic.partition.mode=nonstrict;\n\
    insert overwrite table {DATA_BASE}.dwd_trade_order_detail_inc partition (dt)\n\
    select detail.id,\n\
           detail.order_id,\n\
           info.user_id,\n\
           detail.sku_id,\n\
           info.province_id,\n\
           activity.activity_id,\n\
           activity.activity_rule_id,\n\
           coupon.coupon_id,\n\
           date_format(detail.create_time, 'yyyy-MM-dd')  as date_id,\n\
           detail.create_time,\n\
           detail.source_id,\n\
           detail.source_type                             as source_type_code,\n\
           dic.dic_name                                   as source_type_name,\n\
           detail.sku_num,\n\
           detail.split_original_amount,\n\
           detail.split_activity_amount,\n\
           detail.split_coupon_amount,\n\
           detail.split_total_amount,\n\
           date_format(detail.create_time, 'yyyy-MM-dd')  as dt\n\
    from\n\
    (\n\
        select data.id,\n\
               data.order_id,\n\
               data.sku_id,\n\
               data.create_time,\n\
               data.source_id,\n\
               data.source_type,\n\
               data.sku_num,\n\
               data.sku_num * data.order_price as split_original_amount,\n\
               data.split_total_amount,\n\
               data.split_activity_amount,\n\
               data.split_coupon_amount\n\
        from {DATA_BASE}.ods_order_detail_inc\n\
        where dt = '{start_date}' and type = 'bootstrap-insert'\n\
    ) as detail left join\n\
    (\n\
        select data.id,\n\
               data.user_id,\n\
               data.province_id\n\
        from {DATA_BASE}.ods_order_info_inc\n\
        where dt = '{start_date}' and type = 'bootstrap-insert'\n\
    ) as info\n\
        on detail.order_id = info.id\n\
    left join\n\
    (\n\
        select data.order_detail_id,\n\
               data.activity_id,\n\
               data.activity_rule_id\n\
        from {DATA_BASE}.ods_order_detail_activity_inc\n\
        where dt = '{start_date}' and type = 'bootstrap-insert'\n\
    ) as activity\n\
        on detail.id = activity.order_detail_id\n\
    left join\n\
    (\n\
        select data.order_detail_id,\n\
               data.coupon_id\n\
        from {DATA_BASE}.ods_order_detail_coupon_inc\n\
        where dt = '{start_date}' and type = 'bootstrap-insert'\n\
    ) as coupon\n\
        on detail.id = coupon.order_detail_id\n\
    left join\n\
    (\n\
        select dic_code,\n\
               dic_name\n\
        from {DATA_BASE}.ods_base_dic_full\n\
        where dt = '{start_date}' and parent_code = '24'\n\
    ) as dic on detail.source_type = dic.dic_code;\n\
    set hive.exec.dynamic.partition.mode=strict;"

dwd_trade_cancel_detail_inc = f"\n\
    set hive.exec.dynamic.partition.mode=nonstrict;\n\
    insert overwrite table {DATA_BASE}.dwd_trade_cancel_detail_inc partition (dt)\n\
    select detail.id,\n\
           detail.order_id,\n\
           info.user_id,\n\
           detail.sku_id,\n\
           info.province_id,\n\
           activity.activity_id,\n\
           activity.activity_rule_id,\n\
           coupon.coupon_id,\n\
           date_format(info.canel_time, 'yyyy-MM-dd') as date_id,\n\
           info.canel_time,\n\
           detail.source_id ,\n\
           detail.source_type                         as source_type_code,\n\
           dic.dic_name                               as source_type_name,\n\
           detail.sku_num,\n\
           detail.split_original_amount,\n\
           detail.split_activity_amount,\n\
           detail.split_coupon_amount,\n\
           detail.split_total_amount,\n\
           date_format(info.canel_time, 'yyyy-MM-dd') as dt\n\
    from\n\
    (\n\
        select data.id,\n\
               data.order_id,\n\
               data.sku_id,\n\
               data.source_id,\n\
               data.source_type,\n\
               data.sku_num,\n\
               data.sku_num * data.order_price as split_original_amount,\n\
               data.split_total_amount,\n\
               data.split_activity_amount,\n\
               data.split_coupon_amount\n\
        from {DATA_BASE}.ods_order_detail_inc\n\
        where dt = '{start_date}' and type = 'bootstrap-insert'\n\
    ) as detail join\n\
    (\n\
        select data.id,\n\
               data.user_id,\n\
               data.province_id,\n\
               data.operate_time  as canel_time\n\
        from {DATA_BASE}.ods_order_info_inc\n\
        where dt = '{start_date}' and type = 'bootstrap-insert' and data.order_status = '1003'\n\
    ) as info\n\
        on detail.order_id = info.id\n\
    left join\n\
    (\n\
        select data.order_detail_id,\n\
               data.activity_id,\n\
               data.activity_rule_id\n\
        from {DATA_BASE}.ods_order_detail_activity_inc\n\
        where dt = '{start_date}' and type = 'bootstrap-insert'\n\
    ) as activity\n\
        on detail.id = activity.order_detail_id\n\
    left join\n\
    (\n\
        select data.order_detail_id,\n\
               data.coupon_id\n\
        from {DATA_BASE}.ods_order_detail_coupon_inc\n\
        where dt = '{start_date}' and type = 'bootstrap-insert'\n\
    ) as coupon\n\
        on detail.id = coupon.order_detail_id\n\
    left join\n\
    (\n\
        select dic_code,\n\
               dic_name\n\
        from {DATA_BASE}.ods_base_dic_full\n\
        where dt = '{start_date}' and parent_code = '24'\n\
    ) as dic on detail.source_type = dic.dic_code;\n\
    set hive.exec.dynamic.partition.mode=strict;"

dwd_trade_pay_detail_suc_inc = f"\n\
    insert overwrite table {DATA_BASE}.dwd_trade_pay_detail_suc_inc partition (dt)\n\
    select detail.id,\n\
           detail.order_id,\n\
           payment.user_id,\n\
           detail.sku_id,\n\
           info.province_id,\n\
           activity.activity_id,\n\
           activity.activity_rule_id,\n\
           coupon.coupon_id,\n\
           payment.payment_type                             as payment_type_code,\n\
           pay_dic.dic_name                                 as payment_type_name,\n\
           date_format(payment.callback_time, 'yyyy-MM-dd') as date_id,\n\
           payment.callback_time,\n\
           detail.source_id,\n\
           detail.source_type                               as source_type_code,\n\
           src_dic.dic_name                                 as source_type_name,\n\
           detail.sku_num,\n\
           detail.split_original_amount,\n\
           detail.split_activity_amount,\n\
           detail.split_coupon_amount,\n\
           detail.split_total_amount,\n\
           date_format(payment.callback_time, 'yyyy-MM-dd') as dt\n\
    from\n\
    (\n\
        select data.id,\n\
               data.order_id,\n\
               data.sku_id,\n\
               data.source_id,\n\
               data.source_type,\n\
               data.sku_num,\n\
               data.sku_num * data.order_price as split_original_amount,\n\
               data.split_total_amount,\n\
               data.split_activity_amount,\n\
               data.split_coupon_amount\n\
        from {DATA_BASE}.ods_order_detail_inc\n\
        where dt = '{start_date}' and type = 'bootstrap-insert'\n\
    ) as detail join\n\
    (\n\
        select data.user_id,\n\
               data.order_id,\n\
               data.payment_type,\n\
               data.callback_time\n\
        from {DATA_BASE}.ods_payment_info_inc\n\
        where dt = '{start_date}' and type = 'bootstrap-insert' and data.payment_status = '1602'\n\
    ) as payment\n\
        on detail.order_id = payment.order_id\n\
    left join\n\
    (\n\
        select data.id,\n\
               data.province_id\n\
        from {DATA_BASE}.ods_order_info_inc\n\
        where dt = '{start_date}' and type = 'bootstrap-insert'\n\
    ) as info\n\
        on detail.order_id = info.id\n\
    left join\n\
    (\n\
        select data.order_detail_id,\n\
               data.activity_id,\n\
               data.activity_rule_id\n\
        from {DATA_BASE}.ods_order_detail_activity_inc\n\
        where dt = '{start_date}' and type = 'bootstrap-insert'\n\
    ) as activity\n\
        on detail.id = activity.order_detail_id\n\
    left join\n\
    (\n\
        select data.order_detail_id,\n\
               data.coupon_id\n\
        from {DATA_BASE}.ods_order_detail_coupon_inc\n\
        where dt = '{start_date}' and type = 'bootstrap-insert'\n\
    ) as coupon\n\
        on detail.id = coupon.order_detail_id\n\
    left join\n\
    (\n\
        select dic_code,\n\
               dic_name\n\
        from {DATA_BASE}.ods_base_dic_full\n\
        where dt = '{start_date}' and parent_code = '11'\n\
    ) as pay_dic\n\
        on payment.payment_type = pay_dic.dic_code\n\
    left join\n\
    (\n\
        select dic_code,\n\
               dic_name\n\
        from {DATA_BASE}.ods_base_dic_full\n\
        where dt = '{start_date}' and parent_code = '24'\n\
    ) as src_dic on detail.source_type = src_dic.dic_code;"

dwd_trade_order_refund_inc = f"\n\
    insert overwrite table {DATA_BASE}.dwd_trade_order_refund_inc partition (dt)\n\
    select refund.id,\n\
           refund.user_id,\n\
           refund.order_id,\n\
           refund.sku_id,\n\
           order_info.province_id,\n\
           date_format(refund.create_time, 'yyyy-MM-dd') as date_id,\n\
           refund.create_time,\n\
           refund.refund_type                            as refund_type_code,\n\
           type_dic.dic_name                             as refund_type_name,\n\
           refund.refund_reason_type                     as refund_reason_type_code,\n\
           reason_dic.dic_name                           as refund_reason_type_name,\n\
           refund.refund_reason_txt,\n\
           refund.refund_num,\n\
           refund.refund_amount,\n\
           date_format(refund.create_time, 'yyyy-MM-dd') as dt\n\
    from\n\
    (\n\
        select data.id,\n\
               data.user_id,\n\
               data.order_id,\n\
               data.sku_id,\n\
               data.refund_type,\n\
               data.refund_num,\n\
               data.refund_amount,\n\
               data.refund_reason_type,\n\
               data.refund_reason_txt,\n\
               data.create_time\n\
        from {DATA_BASE}.ods_order_refund_info_inc\n\
        where dt = '{start_date}' and type = 'bootstrap-insert'\n\
    ) as refund left join\n\
    (\n\
        select data.id,\n\
               data.province_id\n\
        from {DATA_BASE}.ods_order_info_inc\n\
        where dt = '{start_date}' and type = 'bootstrap-insert'\n\
    ) as order_info\n\
        on refund.order_id = order_info.id\n\
    left join\n\
    (\n\
        select dic_code,\n\
               dic_name\n\
        from {DATA_BASE}.ods_base_dic_full\n\
        where dt = '{start_date}' and parent_code = '15'\n\
    ) as type_dic\n\
        on refund.refund_type = type_dic.dic_code\n\
    left join\n\
    (\n\
        select dic_code,\n\
               dic_name\n\
        from {DATA_BASE}.ods_base_dic_full\n\
        where dt = '{start_date}' and parent_code = '13'\n\
    ) as reason_dic on refund.refund_reason_type = reason_dic.dic_code;"

dwd_trade_refund_pay_suc_inc = f"\n\
    insert overwrite table {DATA_BASE}.dwd_trade_refund_pay_suc_inc partition (dt)\n\
    select refund_payment.id,\n\
           order_info.user_id,\n\
           refund_payment.order_id,\n\
           refund_payment.sku_id,\n\
           order_info.province_id,\n\
           refund_payment.payment_type              as payment_type_code,\n\
           base_dic.dic_name                        as payment_type_name,\n\
           date_format(callback_time, 'yyyy-MM-dd') as date_id,\n\
           refund_payment.callback_time,\n\
           refund_info.refund_num,\n\
           refund_payment.total_amount,\n\
           date_format(callback_time, 'yyyy-MM-dd') as dt\n\
    from\n\
    (\n\
        select data.id,\n\
               data.order_id,\n\
               data.sku_id,\n\
               data.payment_type,\n\
               data.callback_time,\n\
               data.total_amount\n\
        from {DATA_BASE}.ods_refund_payment_inc\n\
        where dt = '{start_date}' and type = 'bootstrap-insert' and data.refund_status = '1602'\n\
    ) as refund_payment left join\n\
    (\n\
        select data.id,\n\
               data.user_id,\n\
               data.province_id\n\
        from {DATA_BASE}.ods_order_info_inc\n\
        where dt = '{start_date}' and type = 'bootstrap-insert'\n\
    ) as order_info\n\
        on refund_payment.order_id = order_info.id\n\
    left join\n\
    (\n\
        select data.order_id,\n\
               data.sku_id,\n\
               data.refund_num\n\
        from {DATA_BASE}.ods_order_refund_info_inc\n\
        where dt = '{start_date}' and type = 'bootstrap-insert'\n\
    ) as refund_info\n\
        on      refund_payment.order_id = refund_info.order_id\n\
            and refund_payment.sku_id   = refund_info.sku_id\n\
    left join\n\
    (\n\
        select dic_code,\n\
               dic_name\n\
        from {DATA_BASE}.ods_base_dic_full\n\
        where dt = '{start_date}' and parent_code = '11'\n\
    ) as base_dic on refund_payment.payment_type = base_dic.dic_code;"

dwd_trade_cart_full = f"\n\
    insert overwrite table {DATA_BASE}.dwd_trade_cart_full partition (dt = '{start_date}')\n\
    select id,\n\
           user_id,\n\
           sku_id,\n\
           sku_name,\n\
           sku_num\n\
    from {DATA_BASE}.ods_cart_info_full\n\
    where dt = '{start_date}' and is_ordered = '0';"

dwd_tool_coupon_get_inc = f"\n\
    insert overwrite table {DATA_BASE}.dwd_tool_coupon_get_inc partition (dt)\n\
    select data.id,\n\
           data.coupon_id,\n\
           data.user_id,\n\
           date_format(data.get_time, 'yyyy-MM-dd') as date_id,\n\
           data.get_time,\n\
           date_format(data.get_time, 'yyyy-MM-dd') as dt\n\
    from {DATA_BASE}.ods_coupon_use_inc\n\
    where dt = '{start_date}' and type = 'bootstrap-insert';"

dwd_tool_coupon_order_inc = f"\n\
    insert overwrite table {DATA_BASE}.dwd_tool_coupon_order_inc partition (dt)\n\
    select data.id,\n\
           data.coupon_id,\n\
           data.user_id,\n\
           data.order_id,\n\
           date_format(data.using_time, 'yyyy-MM-dd') as date_id,\n\
           data.using_time                            as order_time,\n\
           date_format(data.using_time, 'yyyy-MM-dd') as dt\n\
    from {DATA_BASE}.ods_coupon_use_inc\n\
    where dt = '{start_date}' and type = 'bootstrap-insert' and data.using_time is not null;"

dwd_tool_coupon_pay_inc = f"\n\
    insert overwrite table {DATA_BASE}.dwd_tool_coupon_pay_inc partition (dt)\n\
    select data.id,\n\
           data.coupon_id,\n\
           data.user_id,\n\
           data.order_id,\n\
           date_format(data.used_time, 'yyyy-MM-dd') as date_id,\n\
           data.used_time                            as payment_time,\n\
           date_format(data.used_time, 'yyyy-MM-dd') as dt\n\
    from {DATA_BASE}.ods_coupon_use_inc\n\
    where dt = '{start_date}' and type = 'bootstrap-insert' and data.used_time is not null;"

dwd_interaction_favor_add_inc = f"\n\
    set hive.exec.dynamic.partition.mode=nonstrict;\n\
    insert overwrite table {DATA_BASE}.dwd_interaction_favor_add_inc partition (dt)\n\
    select data.id,\n\
           data.user_id,\n\
           data.sku_id,\n\
           date_format(data.create_time, 'yyyy-MM-dd') as date_id,\n\
           data.create_time,\n\
           date_format(data.create_time, 'yyyy-MM-dd') as dt\n\
    from {DATA_BASE}.ods_favor_info_inc\n\
    where dt = '{start_date}' and type = 'bootstrap-insert';\n\
    set hive.exec.dynamic.partition.mode=strict;"

dwd_interaction_comment_inc = f"\n\
    insert overwrite table {DATA_BASE}.dwd_interaction_comment_inc partition (dt)\n\
    select comment_info.id,\n\
           comment_info.user_id,\n\
           comment_info.sku_id,\n\
           comment_info.order_id,\n\
           date_format(comment_info.create_time, 'yyyy-MM-dd') as date_id,\n\
           comment_info.create_time,\n\
           comment_info.appraise                               as appraise_code,\n\
           base_dic.dic_name                                   as appraise_name,\n\
           date_format(comment_info.create_time, 'yyyy-MM-dd') as dt\n\
    from\n\
    (\n\
        select data.id,\n\
               data.user_id,\n\
               data.sku_id,\n\
               data.order_id,\n\
               data.create_time,\n\
               data.appraise\n\
        from {DATA_BASE}.ods_comment_info_inc\n\
        where dt = '{start_date}' and type = 'bootstrap-insert'\n\
    ) as comment_info left join\n\
    (\n\
        select dic_code,\n\
               dic_name\n\
        from {DATA_BASE}.ods_base_dic_full\n\
        where dt = '{start_date}' and parent_code = '12'\n\
    ) as base_dic on comment_info.appraise = base_dic.dic_code;"

dwd_traffic_page_view_inc = f"\n\
    set hive.cbo.enable=false;\n\
    insert overwrite table {DATA_BASE}.dwd_traffic_page_view_inc partition (dt = '{start_date}')\n\
    select base_province.province_id,\n\
           log.brand,\n\
           log.channel,\n\
           log.is_new,\n\
           log.model,\n\
           log.mid_id,\n\
           log.operate_system,\n\
           log.user_id,\n\
           log.version_code,\n\
           log.page_item,\n\
           log.page_item_type,\n\
           log.last_page_id,\n\
           log.page_id,\n\
           log.source_type,\n\
           date_format(from_utc_timestamp(log.ts, 'GMT+8'), 'yyyy-MM-dd')                                                    as date_id,\n\
           date_format(from_utc_timestamp(log.ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss')                                           as view_time,\n\
           concat(log.mid_id, '-', last_value(log.session_start_point, true) over (partition by log.mid_id order by log.ts)) as session_id,\n\
           during_time\n\
    from\n\
    (\n\
        select common.ar                               as area_code,\n\
               common.ba                               as brand,\n\
               common.ch                               as channel,\n\
               common.is_new                           as is_new,\n\
               common.md                               as model,\n\
               common.mid                              as mid_id,\n\
               common.os                               as operate_system,\n\
               common.uid                              as user_id,\n\
               common.vc                               as version_code,\n\
               page.during_time,\n\
               page.item                               as page_item,\n\
               page.item_type                          as page_item_type,\n\
               page.last_page_id,\n\
               page.page_id,\n\
               page.source_type,\n\
               ts,\n\
               if(page.last_page_id is null, ts, null) as session_start_point\n\
        from {DATA_BASE}.ods_log_inc\n\
        where dt = '{start_date}' and page is not null\n\
    ) as log left join\n\
    (\n\
        select id        as province_id,\n\
               area_code\n\
        from {DATA_BASE}.ods_base_province_full\n\
        where dt = '{start_date}'\n\
    ) as base_province on log.area_code = base_province.area_code;\n\
    set hive.cbo.enable=true;"

dwd_traffic_start_inc = f"\n\
    set hive.cbo.enable=false;\n\
    insert overwrite table {DATA_BASE}.dwd_traffic_start_inc partition (dt = '{start_date}')\n\
    select base_province.province_id,\n\
           log.brand,\n\
           log.channel,\n\
           log.is_new,\n\
           log.model,\n\
           log.mid_id,\n\
           log.operate_system,\n\
           log.user_id,\n\
           log.version_code,\n\
           log.entry,\n\
           log.open_ad_id,\n\
           date_format(from_utc_timestamp(log.ts, 'GMT+8'), 'yyyy-MM-dd')          as date_id,\n\
           date_format(from_utc_timestamp(log.ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') as start_time,\n\
           log.loading_time                                                        as loading_time_ms,\n\
           log.open_ad_ms,\n\
           log.open_ad_skip_ms\n\
    from\n\
    (\n\
        select common.ar               as area_code,\n\
               common.ba               as brand,\n\
               common.ch               as channel,\n\
               common.is_new,\n\
               common.md               as model,\n\
               common.mid              as mid_id,\n\
               common.os               as operate_system,\n\
               common.uid              as user_id,\n\
               common.vc               as version_code,\n\
               \`start\`.entry,\n\
               \`start\`.loading_time,\n\
               \`start\`.open_ad_id,\n\
               \`start\`.open_ad_ms,\n\
               \`start\`.open_ad_skip_ms,\n\
               ts\n\
        from {DATA_BASE}.ods_log_inc\n\
        where dt = '{start_date}' and\n\`start\n\` is not null\n\
    ) as log left join\n\
    (\n\
        select id province_id,\n\
               area_code\n\
        from {DATA_BASE}.ods_base_province_full\n\
        where dt = '{start_date}'\n\
    ) as base_province on log.area_code = base_province.area_code;\n\
    set hive.cbo.enable=true;"

dwd_traffic_action_inc = f"\n\
    set hive.cbo.enable=false;\n\
    insert overwrite table {DATA_BASE}.dwd_traffic_action_inc partition (dt = '{start_date}')\n\
    select base_province.province_id,\n\
           log.brand,\n\
           log.channel,\n\
           log.is_new,\n\
           log.model,\n\
           log.mid_id,\n\
           log.operate_system,\n\
           log.user_id,\n\
           log.version_code,\n\
           log.during_time,\n\
           log.page_item,\n\
           log.page_item_type,\n\
           log.last_page_id,\n\
           log.page_id,\n\
           log.source_type,\n\
           log.action_id,\n\
           log.action_item,\n\
           log.action_item_type,\n\
           date_format(from_utc_timestamp(log.ts, 'GMT+8'), 'yyyy-MM-dd')          as date_id,\n\
           date_format(from_utc_timestamp(log.ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') as action_time\n\
    from\n\
    (\n\
        select common.ar          as area_code,\n\
               common.ba          as brand,\n\
               common.ch          as channel,\n\
               common.is_new,\n\
               common.md          as model,\n\
               common.mid         as mid_id,\n\
               common.os          as operate_system,\n\
               common.uid         as user_id,\n\
               common.vc          as version_code,\n\
               page.during_time,\n\
               page.item          as page_item,\n\
               page.item_type     as page_item_type,\n\
               page.last_page_id,\n\
               page.page_id,\n\
               page.source_type,\n\
               action.action_id,\n\
               action.item        as action_item,\n\
               action.item_type   as action_item_type,\n\
               action.ts\n\
        from {DATA_BASE}.ods_log_inc lateral view explode(actions) tmp as action\n\
        where dt = '{start_date}' and actions is not null\n\
    ) as log left join\n\
    (\n\
        select id province_id,\n\
               area_code\n\
        from {DATA_BASE}.ods_base_province_full\n\
        where dt = '{start_date}'\n\
    ) base_province on log.area_code = base_province.area_code;\n\
    set hive.cbo.enable=true;"

dwd_traffic_display_inc = f"\n\
    set hive.cbo.enable=false;\n\
    insert overwrite table {DATA_BASE}.dwd_traffic_display_inc partition (dt = '{start_date}')\n\
    select base_province.province_id,\n\
           log.brand,\n\
           log.channel,\n\
           log.is_new,\n\
           log.model,\n\
           log.mid_id,\n\
           log.operate_system,\n\
           log.user_id,\n\
           log.version_code,\n\
           log.during_time,\n\
           log.page_item,\n\
           log.page_item_type,\n\
           log.last_page_id,\n\
           log.page_id,\n\
           log.source_type,\n\
           date_format(from_utc_timestamp(log.ts, 'GMT+8'), 'yyyy-MM-dd')          as date_id,\n\
           date_format(from_utc_timestamp(log.ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') as display_time,\n\
           log.display_type,\n\
           log.display_item,\n\
           log.display_item_type,\n\
           log.display_order,\n\
           log.display_pos_id\n\
    from\n\
    (\n\
        select common.ar             as area_code,\n\
               common.ba             as brand,\n\
               common.ch             as channel,\n\
               common.is_new,\n\
               common.md             as model,\n\
               common.mid            as mid_id,\n\
               common.os             as operate_system,\n\
               common.uid            as user_id,\n\
               common.vc             as version_code,\n\
               page.during_time,\n\
               page.item             as page_item,\n\
               page.item_type        as page_item_type,\n\
               page.last_page_id,\n\
               page.page_id,\n\
               page.source_type,\n\
               display.display_type,\n\
               display.item          as display_item,\n\
               display.item_type     as display_item_type,\n\
               display.\n\`order\n\`        as display_order,\n\
               display.pos_id        as display_pos_id,\n\
               ts\n\
        from {DATA_BASE}.ods_log_inc lateral view explode(displays) tmp as display\n\
        where dt = '{start_date}' and displays is not null\n\
    ) as log left join\n\
    (\n\
        select id province_id,\n\
               area_code\n\
        from {DATA_BASE}.ods_base_province_full\n\
        where dt = '{start_date}'\n\
    ) as base_province on log.area_code = base_province.area_code;\n\
    set hive.cbo.enable=true;"

dwd_traffic_error_inc = f"\n\
    set hive.cbo.enable=false;\n\
    set hive.execution.engine=mr;\n\
    insert overwrite table {DATA_BASE}.dwd_traffic_error_inc partition (dt = '{start_date}')\n\
    select base_province.province_id,\n\
           log.brand,\n\
           log.channel,\n\
           log.is_new,\n\
           log.model,\n\
           log.mid_id,\n\
           log.operate_system,\n\
           log.user_id,\n\
           log.version_code,\n\
           log.page_item,\n\
           log.page_item_type,\n\
           log.last_page_id,\n\
           log.page_id,\n\
           log.source_type,\n\
           log.entry,\n\
           log.loading_time,\n\
           log.open_ad_id,\n\
           log.open_ad_ms,\n\
           log.open_ad_skip_ms,\n\
           log.actions,\n\
           log.displays,\n\
           date_format(from_utc_timestamp(log.ts, 'GMT+8'), 'yyyy-MM-dd')          as date_id,\n\
           date_format(from_utc_timestamp(log.ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') as error_time,\n\
           log.error_code,\n\
           log.error_msg\n\
    from\n\
    (\n\
        select common.ar             as area_code,\n\
               common.ba             as brand,\n\
               common.ch             as channel,\n\
               common.is_new,\n\
               common.md             as model,\n\
               common.mid            as mid_id,\n\
               common.os             as operate_system,\n\
               common.uid            as user_id,\n\
               common.vc             as version_code,\n\
               page.during_time,\n\
               page.item             as page_item,\n\
               page.item_type        as page_item_type,\n\
               page.last_page_id,\n\
               page.page_id,\n\
               page.source_type,\n\
               \`start\`.entry,\n\
               \`start\`.loading_time,\n\
               \`start\`.open_ad_id,\n\
               \`start\`.open_ad_ms,\n\
               \`start\`.open_ad_skip_ms,\n\
               actions,\n\
               displays,\n\
               err.error_code,\n\
               err.msg               as error_msg,\n\
               ts\n\
        from {DATA_BASE}.ods_log_inc\n\
        where dt = '{start_date}' and err is not null\n\
    ) as log left join\n\
    (\n\
        select id province_id,\n\
               area_code\n\
        from {DATA_BASE}.ods_base_province_full\n\
        where dt = '{start_date}'\n\
    ) as base_province on log.area_code = base_province.area_code;\n\
    set hive.cbo.enable=true;\n\
    set hive.execution.engine=spark;"

dwd_user_register_inc = f"\n\
    set hive.exec.dynamic.partition.mode=nonstrict;\n\
    insert overwrite table {DATA_BASE}.dwd_user_register_inc partition (dt)\n\
    select user_info.user_id,\n\
           date_format(user_info.create_time, 'yyyy-MM-dd') as date_id,\n\
           user_info.create_time,\n\
           log.channel,\n\
           base_province.province_id,\n\
           log.version_code,\n\
           log.mid_id,\n\
           log.brand,\n\
           log.model,\n\
           log.operate_system,\n\
           date_format(user_info.create_time, 'yyyy-MM-dd') as dt\n\
    from\n\
    (\n\
        select data.id user_id,\n\
               data.create_time\n\
        from {DATA_BASE}.ods_user_info_inc\n\
        where dt = '{start_date}' and type = 'bootstrap-insert'\n\
    ) user_info left join\n\
    (\n\
        select common.ar  as area_code,\n\
               common.ba  as brand,\n\
               common.ch  as channel,\n\
               common.md  as model,\n\
               common.mid as mid_id,\n\
               common.os  as operate_system,\n\
               common.uid as user_id,\n\
               common.vc  as version_code\n\
        from {DATA_BASE}.ods_log_inc\n\
        where dt = '{start_date}' and page.page_id = 'register' and common.uid is not null\n\
    ) as log\n\
        on user_info.user_id = log.user_id\n\
    left join\n\
    (\n\
        select id province_id,\n\
               area_code\n\
        from {DATA_BASE}.ods_base_province_full\n\
        where dt = '{start_date}'\n\
    ) base_province on log.area_code = base_province.area_code;\n\
    set hive.exec.dynamic.partition.mode=strict;"

dwd_user_login_inc = f"\n\
    insert overwrite table {DATA_BASE}.dwd_user_login_inc partition (dt = '{start_date}')\n\
    select user_id,\n\
           date_format(from_utc_timestamp(log.ts, 'GMT+8'), 'yyyy-MM-dd')          as date_id,\n\
           date_format(from_utc_timestamp(log.ts, 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') as login_time,\n\
           log.channel,\n\
           base_province.province_id,\n\
           log.version_code,\n\
           log.mid_id,\n\
           log.brand,\n\
           log.model,\n\
           log.operate_system\n\
    from\n\
    (\n\
        select v.user_id,\n\
               v.channel,\n\
               v.area_code,\n\
               v.version_code,\n\
               v.mid_id,\n\
               v.brand,\n\
               v.model,\n\
               v.operate_system,\n\
               v.ts\n\
        from\n\
        (\n\
            select u.user_id,\n\
                   u.channel,\n\
                   u.area_code,\n\
                   u.version_code,\n\
                   u.mid_id,\n\
                   u.brand,\n\
                   u.model,\n\
                   u.operate_system,\n\
                   u.ts,\n\
                   row_number() over (partition by u.session_id order by u.ts) as rn\n\
            from\n\
            (\n\
                select t.user_id,\n\
                       t.channel,\n\
                       t.area_code,\n\
                       t.version_code,\n\
                       t.mid_id,\n\
                       t.brand,\n\
                       t.model,\n\
                       t.operate_system,\n\
                       t.ts,\n\
                       concat(t.mid_id, '-', last_value(t.session_start_point, true) over (partition by t.mid_id order by t.ts)) as session_id\n\
                from\n\
                (\n\
                    select common.uid                              as user_id,\n\
                           common.ch                               as channel,\n\
                           common.ar                               as area_code,\n\
                           common.vc                               as version_code,\n\
                           common.mid                              as mid_id,\n\
                           common.ba                               as brand,\n\
                           common.md                               as model,\n\
                           common.os                               as operate_system,\n\
                           ts,\n\
                           if(page.last_page_id is null, ts, null) as session_start_point\n\
                    from {DATA_BASE}.ods_log_inc\n\
                    where dt = '{start_date}' and page is not null\n\
                ) as t\n\
            ) as u where user_id is not null\n\
        ) as v where rn = 1\n\
    ) as log left join\n\
    (\n\
        select id province_id,\n\
               area_code\n\
        from {DATA_BASE}.ods_base_province_full\n\
        where dt = '{start_date}'\n\
    ) as base_province on log.area_code = base_province.area_code;"
