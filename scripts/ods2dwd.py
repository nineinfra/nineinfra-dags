def get_ods2dwd_sqls(data_base, start_date):
    dwd_trade_cart_add_inc = f"""
        insert overwrite table {data_base}.dwd_trade_cart_add_inc partition (dt = '{start_date}')
        select cart.id,
               cart.user_id,
               cart.sku_id,
               cart.date_id,
               cart.create_time,
               cart.source_id,
               cart.source_type                            as source_type_code,
               dic.dic_name                                as source_type_name,
               cart.sku_num
        from
        (
            select data.id,
                   data.user_id,
                   data.sku_id,
                   date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyyMM-dd')          as date_id,
                   date_format(from_utc_timestamp(ts, 'GMT+8'), 'yyyyMM-dd HH:mm:ss') as create_time,
                   data.source_id,
                   data.source_type,
                   if(type = 'insert', data.sku_num, data.sku_num-old['sku_num'])             as sku_num
            from {data_base}.ods_cart_info_inc
            where   dt    = '{start_date}'
                and (type = 'insert' or (type = 'update' and old['sku_num'] is not null and data.sku_num > cast(old['sku_num'] as int)))
        ) as cart left join
        (
            select dic_code,
                   dic_name
            from {data_base}.ods_base_dic_full
            where dt = '{start_date}' and parent_code = '24'
        ) as dic on cart.source_type = dic.dic_code;"""

    dwd_trade_order_detail_inc = f"""
        insert overwrite table {data_base}.dwd_trade_order_detail_inc partition (dt = '{start_date}')
        select detail.id,
               detail.order_id,
               info.user_id,
               detail.sku_id,
               info.province_id,
               activity.activity_id,
               activity.activity_rule_id,
               coupon.coupon_id,
               date_format(detail.create_time, 'yyyy-MM-dd')  as date_id,
               detail.create_time,
               detail.source_id,
               detail.source_type                             as source_type_code,
               dic.dic_name                                   as source_type_name,
               detail.sku_num,
               detail.split_original_amount,
               detail.split_activity_amount,
               detail.split_coupon_amount,
               detail.split_total_amount
        from
        (
            select data.id,
                   data.order_id,
                   data.sku_id,
                   data.create_time,
                   data.source_id,
                   data.source_type,
                   data.sku_num,
                   data.sku_num * data.order_price as split_original_amount,
                   data.split_total_amount,
                   data.split_activity_amount,
                   data.split_coupon_amount
            from {data_base}.ods_order_detail_inc
            where dt = '{start_date}' and type = 'insert'
        ) as detail left join
        (
            select data.id,
                   data.user_id,
                   data.province_id
            from {data_base}.ods_order_info_inc
            where dt = '{start_date}' and type = 'insert'
        ) as info
            on detail.order_id = info.id
        left join
        (
            select data.order_detail_id,
                   data.activity_id,
                   data.activity_rule_id
            from {data_base}.ods_order_detail_activity_inc
            where dt = '{start_date}' and type = 'insert'
        ) as activity
            on detail.id = activity.order_detail_id
        left join
        (
            select data.order_detail_id,
                   data.coupon_id
            from {data_base}.ods_order_detail_coupon_inc
            where dt = '{start_date}' and type = 'insert'
        ) as coupon
            on detail.id = coupon.order_detail_id
        left join
        (
            select dic_code,
                   dic_name
            from {data_base}.ods_base_dic_full
            where dt = '{start_date}' and parent_code = '24'
        ) as dic on detail.source_type = dic.dic_code;"""

    dwd_trade_cancel_detail_inc = f"""
        insert overwrite table {data_base}.dwd_trade_cancel_detail_inc partition (dt = '{start_date}')
        select detail.id,
               detail.order_id,
               info.user_id,
               detail.sku_id,
               info.province_id,
               activity.activity_id,
               activity.activity_rule_id,
               coupon.coupon_id,
               date_format(info.canel_time, 'yyyy-MM-dd') as date_id,
               info.canel_time,
               detail.source_id ,
               detail.source_type                         as source_type_code,
               dic.dic_name                               as source_type_name,
               detail.sku_num,
               detail.split_original_amount,
               detail.split_activity_amount,
               detail.split_coupon_amount,
               detail.split_total_amount
        from
        (
            select data.id,
                   data.order_id,
                   data.sku_id,
                   data.source_id,
                   data.source_type,
                   data.sku_num,
                   data.sku_num * data.order_price split_original_amount,
                   data.split_total_amount,
                   data.split_activity_amount,
                   data.split_coupon_amount
            from {data_base}.ods_order_detail_inc
            where (dt = '{start_date}' or dt = date_add('{start_date}', -1)) and (type = 'insert' or type = 'bootstrap-insert')
        ) as detail join
        (
            select data.id,
                   data.user_id,
                   data.province_id,
                   data.operate_time  as canel_time
            from {data_base}.ods_order_info_inc
            where   dt = '{start_date}' and type = 'update' and data.order_status = '1003'
                and array_contains(map_keys(old), 'order_status')
        ) as info
            on detail.order_id = info.id
        left join
        (
            select data.order_detail_id,
                   data.activity_id,
                   data.activity_rule_id
            from {data_base}.ods_order_detail_activity_inc
            where (dt = '{start_date}' or dt = date_add('{start_date}', -1)) and (type = 'insert' or type = 'bootstrap-insert')
        ) as activity
            on detail.id = activity.order_detail_id
        left join
        (
            select data.order_detail_id,
                   data.coupon_id
            from {data_base}.ods_order_detail_coupon_inc
            where (dt = '{start_date}' or dt = date_add('{start_date}', -1)) and (type = 'insert' or type = 'bootstrap-insert')
        ) as coupon
            on detail.id = coupon.order_detail_id
        left join
        (
            select dic_code,
                   dic_name
            from {data_base}.ods_base_dic_full
            where dt = '{start_date}' and parent_code = '24'
        ) as dic on detail.source_type = dic.dic_code;"""

    dwd_trade_pay_detail_suc_inc = f"""
        insert overwrite table {data_base}.dwd_trade_pay_detail_suc_inc partition (dt = '{start_date}')
        select detail.id,
               detail.order_id,
               payment.user_id,
               detail.sku_id,
               info.province_id,
               activity.activity_id,
               activity.activity_rule_id,
               coupon.coupon_id,
               payment.payment_type                             as payment_type_code,
               pay_dic.dic_name                                 as payment_type_name,
               date_format(payment.callback_time, 'yyyy-MM-dd') as date_id,
               payment.callback_time,
               detail.source_id,
               detail.source_type                               as source_type_code,
               src_dic.dic_name                                 as source_type_name,
               detail.sku_num,
               detail.split_original_amount,
               detail.split_activity_amount,
               detail.split_coupon_amount,
               detail.split_total_amount
        from
        (
            select data.id,
                   data.order_id,
                   data.sku_id,
                   data.source_id,
                   data.source_type,
                   data.sku_num,
                   data.sku_num * data.order_price as split_original_amount,
                   data.split_total_amount,
                   data.split_activity_amount,
                   data.split_coupon_amount
            from {data_base}.ods_order_detail_inc
            where (dt = '{start_date}' or dt = date_add('{start_date}', -1)) and (type = 'insert' or type = 'bootstrap-insert')
        ) as detail join
        (
            select data.user_id,
                   data.order_id,
                   data.payment_type,
                   data.callback_time
            from {data_base}.ods_payment_info_inc
            where   dt                  = '{start_date}'
                and data.payment_status = '1602'
                and type                = 'update'
                and array_contains(map_keys(old), 'payment_status')
        ) as payment
            on detail.order_id = payment.order_id
        left join
        (
            select data.id,
                   data.province_id
            from {data_base}.ods_order_info_inc
            where (dt = '{start_date}' or dt = date_add('{start_date}', -1)) and (type = 'insert' or type = 'bootstrap-insert')
        ) as info
            on detail.order_id = info.id
        left join
        (
            select data.order_detail_id,
                   data.activity_id,
                   data.activity_rule_id
            from {data_base}.ods_order_detail_activity_inc
            where (dt = '{start_date}' or dt = date_add('{start_date}', -1)) and (type = 'insert' or type = 'bootstrap-insert')
        ) as activity
            on detail.id = activity.order_detail_id
        left join
        (
            select data.order_detail_id,
                   data.coupon_id
            from {data_base}.ods_order_detail_coupon_inc
            where (dt = '{start_date}' or dt = date_add('{start_date}', -1)) and (type = 'insert' or type = 'bootstrap-insert')
        ) as coupon
            on detail.id = coupon.order_detail_id
        left join
        (
            select dic_code,
                   dic_name
            from {data_base}.ods_base_dic_full
            where dt = '{start_date}' and parent_code = '11'
        ) as pay_dic
            on payment.payment_type = pay_dic.dic_code
        left join
        (
            select dic_code,
                   dic_name
            from {data_base}.ods_base_dic_full
            where dt = '{start_date}' and parent_code = '24'
        ) as src_dic on detail.source_type = src_dic.dic_code;"""

    dwd_trade_order_refund_inc = f"""
        insert overwrite table {data_base}.dwd_trade_order_refund_inc partition (dt = '{start_date}')
        select refund.id,
               refund.user_id,
               refund.order_id,
               refund.sku_id,
               order_info.province_id,
               date_format(refund.create_time, 'yyyy-MM-dd') as date_id,
               refund.create_time,
               refund.refund_type                            as refund_type_code,
               type_dic.dic_name                             as refund_type_name,
               refund.refund_reason_type                     as refund_reason_type_code,
               reason_dic.dic_name                           as refund_reason_type_name,
               refund.refund_reason_txt,
               refund.refund_num,
               refund.refund_amount
        from
        (
            select data.id,
                   data.user_id,
                   data.order_id,
                   data.sku_id,
                   data.refund_type,
                   data.refund_num,
                   data.refund_amount,
                   data.refund_reason_type,
                   data.refund_reason_txt,
                   data.create_time
            from {data_base}.ods_order_refund_info_inc
            where dt = '{start_date}' and type = 'insert'
        ) as refund left join
        (
            select data.id,
                   data.province_id
            from {data_base}.ods_order_info_inc
            where                dt   = '{start_date}'
                             and type = 'update'
                and data.order_status = '1005'
                and array_contains(map_keys(old), 'order_status')
        ) as order_info
            on refund.order_id = order_info.id
        left join
        (
            select dic_code,
                   dic_name
            from {data_base}.ods_base_dic_full
            where dt = '{start_date}' and parent_code = '15'
        ) as type_dic
            on refund.refund_type = type_dic.dic_code
        left join
        (
            select dic_code,
                   dic_name
            from {data_base}.ods_base_dic_full
            where dt = '{start_date}' and parent_code = '13'
        ) reason_dic on refund.refund_reason_type = reason_dic.dic_code;"""

    dwd_trade_refund_pay_suc_inc = f"""
        insert overwrite table {data_base}.dwd_trade_refund_pay_suc_inc partition (dt = '{start_date}')
        select refund_payment.id,
               order_info.user_id,
               refund_payment.order_id,
               refund_payment.sku_id,
               order_info.province_id,
               refund_payment.payment_type              as payment_type_code,
               base_dic.dic_name                        as payment_type_name,
               date_format(callback_time, 'yyyy-MM-dd') as date_id,
               refund_payment.callback_time,
               refund_info.refund_num,
               refund_payment.total_amount
        from
        (
            select data.id,
                   data.order_id,
                   data.sku_id,
                   data.payment_type,
                   data.callback_time,
                   data.total_amount
            from {data_base}.ods_refund_payment_inc
            where   dt                 = '{start_date}'
                and type               = 'update'
                and data.refund_status = '1602'
                and array_contains(map_keys(old), 'refund_status')
        ) as refund_payment left join
        (
            select data.id,
                   data.user_id,
                   data.province_id
            from {data_base}.ods_order_info_inc
            where   dt                = '{start_date}'
                and type              = 'update'
                and data.order_status = '1006'
                and array_contains(map_keys(old), 'order_status')
        ) as order_info
            on refund_payment.order_id = order_info.id
        left join
        (
            select data.order_id,
                   data.sku_id,
                   data.refund_num
            from {data_base}.ods_order_refund_info_inc
            where   dt                 = '{start_date}'
                and type               = 'update'
                and data.refund_status = '0705'
                and array_contains(map_keys(old), 'refund_status')
        ) as refund_info
            on      refund_payment.order_id = refund_info.order_id
                and refund_payment.sku_id   = refund_info.sku_id
        left join
        (
            select dic_code,
                   dic_name
            from {data_base}.ods_base_dic_full
            where dt = '{start_date}' and parent_code = '11'
        ) as base_dic on refund_payment.payment_type = base_dic.dic_code;"""

    dwd_trade_cart_full = f"""
        insert overwrite table {data_base}.dwd_trade_cart_full partition (dt = '{start_date}')
        select id,
               user_id,
               sku_id,
               sku_name,
               sku_num
        from {data_base}.ods_cart_info_full
        where dt = '{start_date}' and is_ordered = '0';"""

    dwd_tool_coupon_get_inc = f"""
        insert overwrite table {data_base}.dwd_tool_coupon_get_inc partition (dt = '{start_date}')
        select data.id,
               data.coupon_id,
               data.user_id,
               date_format(data.get_time, 'yyyy-MM-dd') as date_id,
               data.get_time
        from {data_base}.ods_coupon_use_inc
        where dt = '{start_date}' and type = 'insert';"""

    dwd_tool_coupon_order_inc = f"""
        insert overwrite table {data_base}.dwd_tool_coupon_order_inc partition (dt = '{start_date}')
        select data.id,
               data.coupon_id,
               data.user_id,
               data.order_id,
               date_format(data.using_time, 'yyyy-MM-dd') as date_id,
               data.using_time                            as order_time
        from {data_base}.ods_coupon_use_inc
        where dt = '{start_date}' and type = 'update' and array_contains(map_keys(old), 'using_time');"""

    dwd_tool_coupon_pay_inc = f"""
        insert overwrite table {data_base}.dwd_tool_coupon_pay_inc partition (dt = '{start_date}')
        select data.id,
               data.coupon_id,
               data.user_id,
               data.order_id,
               date_format(data.used_time, 'yyyy-MM-dd') as date_id,
               data.used_time                            as payment_time
        from {data_base}.ods_coupon_use_inc
        where dt = '{start_date}' and type = 'update' and array_contains(map_keys(old), 'used_time');"""

    dwd_interaction_favor_add_inc = f"""
        insert overwrite table {data_base}.dwd_interaction_favor_add_inc partition (dt = '{start_date}')
        select data.id,
               data.user_id,
               data.sku_id,
               date_format(data.create_time, 'yyyy-MM-dd') as date_id,
               data.create_time
        from {data_base}.ods_favor_info_inc
        where dt = '{start_date}' and type = 'insert';"""

    dwd_interaction_comment_inc = f"""
        insert overwrite table {data_base}.dwd_interaction_comment_inc partition (dt = '{start_date}')
        select comment_info.id,
               comment_info.user_id,
               comment_info.sku_id,
               comment_info.order_id,
               date_format(comment_info.create_time, 'yyyy-MM-dd') as date_id,
               comment_info.create_time,
               comment_info.appraise                               as appraise_code,
               base_dic.dic_name                                   as appraise_name
        from
        (
            select data.id,
                   data.user_id,
                   data.sku_id,
                   data.order_id,
                   data.create_time,
                   data.appraise
            from {data_base}.ods_comment_info_inc
            where dt = '{start_date}' and type = 'insert'
        ) as comment_info left join
        (
            select dic_code,
                   dic_name
            from {data_base}.ods_base_dic_full
            where dt = '{start_date}' and parent_code = '12'
        ) as base_dic on comment_info.appraise = base_dic.dic_code;"""

    dwd_traffic_page_view_inc = f"""
        set hive.cbo.enable=false;
        insert overwrite table {data_base}.dwd_traffic_page_view_inc partition (dt = '{start_date}')
        select base_province.province_id,
               log.brand,
               log.channel,
               log.is_new,
               log.model,
               log.mid_id,
               log.operate_system,
               log.user_id,
               log.version_code,
               log.page_item,
               log.page_item_type,
               log.last_page_id,
               log.page_id,
               log.source_type,
               date_format(from_utc_timestamp(to_timestamp(log.ts), 'GMT+8'), 'yyyy-MM-dd')                                                    as date_id,
               date_format(from_utc_timestamp(to_timestamp(log.ts), 'GMT+8'), 'yyyy-MM-dd HH:mm:ss')                                           as view_time,
               concat(log.mid_id, '-', last_value(log.session_start_point, true) over (partition by log.mid_id order by log.ts)) as session_id,
               CAST(during_time AS BIGINT) AS during_time
        from
        (
            select common.ar                               as area_code,
                   common.ba                               as brand,
                   common.ch                               as channel,
                   common.is_new                           as is_new,
                   common.md                               as model,
                   common.mid                              as mid_id,
                   common.os                               as operate_system,
                   common.uid                              as user_id,
                   common.vc                               as version_code,
                   page.during_time,
                   page.item                               as page_item,
                   page.item_type                          as page_item_type,
                   page.last_page_id,
                   page.page_id,
                   page.source_type,
                   ts,
                   if(page.last_page_id is null, ts, null) as session_start_point
            from {data_base}.ods_log_inc
            where dt = '{start_date}' and page is not null
        ) as log left join
        (
            select id        as province_id,
                   area_code
            from {data_base}.ods_base_province_full
            where dt = '{start_date}'
        ) as base_province on log.area_code = base_province.area_code;
        set hive.cbo.enable=true;"""

    dwd_traffic_start_inc = f"""
        set hive.cbo.enable=false;
        insert overwrite table {data_base}.dwd_traffic_start_inc partition (dt = '{start_date}')
        select base_province.province_id,
               log.brand,
               log.channel,
               log.is_new,
               log.model,
               log.mid_id,
               log.operate_system,
               log.user_id,
               log.version_code,
               log.entry,
               log.open_ad_id,
               date_format(from_utc_timestamp(to_timestamp(log.ts), 'GMT+8'), 'yyyy-MM-dd')          as date_id,
               date_format(from_utc_timestamp(to_timestamp(log.ts), 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') as start_time,
               log.loading_time                                                        as loading_time_ms,
               log.open_ad_ms,
               log.open_ad_skip_ms
        from
        (
            select common.ar               as area_code,
                   common.ba               as brand,
                   common.ch               as channel,
                   common.is_new,
                   common.md               as model,
                   common.mid              as mid_id,
                   common.os               as operate_system,
                   common.uid              as user_id,
                   common.vc               as version_code,
                   `start`.entry,
                   `start`.loading_time,
                   `start`.open_ad_id,
                   `start`.open_ad_ms,
                   `start`.open_ad_skip_ms,
                   ts
            from {data_base}.ods_log_inc
            where dt = '{start_date}' and `start` is not null
        ) as log left join
        (
            select id province_id,
                   area_code
            from {data_base}.ods_base_province_full
            where dt = '{start_date}'
        ) as base_province on log.area_code = base_province.area_code;
        set hive.cbo.enable=true;"""

    dwd_traffic_action_inc = f"""
        set hive.cbo.enable=false;
        insert overwrite table {data_base}.dwd_traffic_action_inc partition (dt = '{start_date}')
        select base_province.province_id,
               log.brand,
               log.channel,
               log.is_new,
               log.model,
               log.mid_id,
               log.operate_system,
               log.user_id,
               log.version_code,
               log.during_time,
               log.page_item,
               log.page_item_type,
               log.last_page_id,
               log.page_id,
               log.source_type,
               log.action_id,
               log.action_item,
               log.action_item_type,
               date_format(from_utc_timestamp(to_timestamp(log.ts), 'GMT+8'), 'yyyy-MM-dd')          as date_id,
               date_format(from_utc_timestamp(to_timestamp(log.ts), 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') as action_time
        from
        (
            select common.ar          as area_code,
                   common.ba          as brand,
                   common.ch          as channel,
                   common.is_new,
                   common.md          as model,
                   common.mid         as mid_id,
                   common.os          as operate_system,
                   common.uid         as user_id,
                   common.vc          as version_code,
                   CAST(page.during_time AS BIGINT) AS during_time,
                   page.item          as page_item,
                   page.item_type     as page_item_type,
                   page.last_page_id,
                   page.page_id,
                   page.source_type,
                   action.action_id,
                   action.item        as action_item,
                   action.item_type   as action_item_type,
                   action.ts
            from {data_base}.ods_log_inc lateral view explode(actions) tmp as action
            where dt = '{start_date}' and actions is not null
        ) as log left join
        (
            select id province_id,
                   area_code
            from {data_base}.ods_base_province_full
            where dt = '{start_date}'
        ) base_province on log.area_code = base_province.area_code;
        set hive.cbo.enable=true;"""

    dwd_traffic_display_inc = f"""
        set hive.cbo.enable=false;
        insert overwrite table {data_base}.dwd_traffic_display_inc partition (dt = '{start_date}')
        select base_province.province_id,
               log.brand,
               log.channel,
               log.is_new,
               log.model,
               log.mid_id,
               log.operate_system,
               log.user_id,
               log.version_code,
               log.during_time,
               log.page_item,
               log.page_item_type,
               log.last_page_id,
               log.page_id,
               log.source_type,
               date_format(from_utc_timestamp(to_timestamp(log.ts), 'GMT+8'), 'yyyy-MM-dd')          as date_id,
               date_format(from_utc_timestamp(to_timestamp(log.ts), 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') as display_time,
               log.display_type,
               log.display_item,
               log.display_item_type,
               log.display_order,
               log.display_pos_id
        from
        (
            select common.ar             as area_code,
                   common.ba             as brand,
                   common.ch             as channel,
                   common.is_new,
                   common.md             as model,
                   common.mid            as mid_id,
                   common.os             as operate_system,
                   common.uid            as user_id,
                   common.vc             as version_code,
                   CAST(page.during_time AS BIGINT) AS during_time,
                   page.item             as page_item,
                   page.item_type        as page_item_type,
                   page.last_page_id,
                   page.page_id,
                   page.source_type,
                   display.display_type,
                   display.item          as display_item,
                   display.item_type     as display_item_type,
                   CAST(display.`order` AS BIGINT)       as display_order,
                   CAST(display.pos_id AS BIGINT)        as display_pos_id,
                   ts
            from {data_base}.ods_log_inc lateral view explode(displays) tmp as display
            where dt = '{start_date}' and displays is not null
        ) as log left join
        (
            select id province_id,
                   area_code
            from {data_base}.ods_base_province_full
            where dt = '{start_date}'
        ) as base_province on log.area_code = base_province.area_code;
        set hive.cbo.enable=true;"""

    dwd_traffic_error_inc = f"""
        set hive.cbo.enable=false;
        set hive.execution.engine=mr;
        insert overwrite table {data_base}.dwd_traffic_error_inc partition (dt = '{start_date}')
        select base_province.province_id,
               log.brand,
               log.channel,
               log.is_new,
               log.model,
               log.mid_id,
               log.operate_system,
               log.user_id,
               log.version_code,
               log.page_item,
               log.page_item_type,
               log.last_page_id,
               log.page_id,
               log.source_type,
               log.entry,
               log.loading_time,
               log.open_ad_id,
               log.open_ad_ms,
               log.open_ad_skip_ms,
               log.actions,
               log.displays,
               date_format(from_utc_timestamp(to_timestamp(log.ts), 'GMT+8'), 'yyyy-MM-dd')          as date_id,
               date_format(from_utc_timestamp(to_timestamp(log.ts), 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') as error_time,
               log.error_code,
               log.error_msg
        from
        (
            select common.ar             as area_code,
                   common.ba             as brand,
                   common.ch             as channel,
                   common.is_new,
                   common.md             as model,
                   common.mid            as mid_id,
                   common.os             as operate_system,
                   common.uid            as user_id,
                   common.vc             as version_code,
                   page.during_time,
                   page.item             as page_item,
                   page.item_type        as page_item_type,
                   page.last_page_id,
                   page.page_id,
                   page.source_type,
                   `start`.entry,
                   `start`.loading_time,
                   `start`.open_ad_id,
                   `start`.open_ad_ms,
                   `start`.open_ad_skip_ms,
                   actions,
                   displays,
                   err.error_code,
                   err.msg               as error_msg,
                   ts
            from {data_base}.ods_log_inc
            where dt = '{start_date}' and err is not null
        ) as log left join
        (
            select id province_id,
                   area_code
            from {data_base}.ods_base_province_full
            where dt = '{start_date}'
        ) as base_province on log.area_code = base_province.area_code;
        set hive.execution.engine=spark;
        set hive.cbo.enable=true;"""

    dwd_user_register_inc = f"""
        insert overwrite table {data_base}.dwd_user_register_inc partition (dt = '{start_date}')
        select user_info.user_id,
               date_format(user_info.create_time, 'yyyy-MM-dd') as date_id,
               user_info.create_time,
               log.channel,
               base_province.province_id,
               log.version_code,
               log.mid_id,
               log.brand,
               log.model,
               log.operate_system
        from
        (
            select data.id user_id,
                   data.create_time
            from {data_base}.ods_user_info_inc
            where dt = '{start_date}' and type = 'insert'
        ) user_info left join
        (
            select common.ar  as area_code,
                   common.ba  as brand,
                   common.ch  as channel,
                   common.md  as model,
                   common.mid as mid_id,
                   common.os  as operate_system,
                   common.uid as user_id,
                   common.vc  as version_code
            from {data_base}.ods_log_inc
            where dt = '{start_date}' and page.page_id = 'register' and common.uid is not null
        ) as log
            on user_info.user_id = log.user_id
        left join
        (
            select id province_id,
                   area_code
            from {data_base}.ods_base_province_full
            where dt = '{start_date}'
        ) base_province on log.area_code = base_province.area_code;"""

    dwd_user_login_inc = f"""
        insert overwrite table {data_base}.dwd_user_login_inc partition (dt = '{start_date}')
        select user_id,
               date_format(from_utc_timestamp(to_timestamp(log.ts), 'GMT+8'), 'yyyy-MM-dd')          as date_id,
               date_format(from_utc_timestamp(to_timestamp(log.ts), 'GMT+8'), 'yyyy-MM-dd HH:mm:ss') as login_time,
               log.channel,
               base_province.province_id,
               log.version_code,
               log.mid_id,
               log.brand,
               log.model,
               log.operate_system
        from
        (
            select v.user_id,
                   v.channel,
                   v.area_code,
                   v.version_code,
                   v.mid_id,
                   v.brand,
                   v.model,
                   v.operate_system,
                   v.ts
            from
            (
                select u.user_id,
                       u.channel,
                       u.area_code,
                       u.version_code,
                       u.mid_id,
                       u.brand,
                       u.model,
                       u.operate_system,
                       u.ts,
                       row_number() over (partition by u.session_id order by u.ts) as rn
                from
                (
                    select t.user_id,
                           t.channel,
                           t.area_code,
                           t.version_code,
                           t.mid_id,
                           t.brand,
                           t.model,
                           t.operate_system,
                           t.ts,
                           concat(t.mid_id, '-', last_value(t.session_start_point, true) over (partition by t.mid_id order by t.ts)) as session_id
                    from
                    (
                        select common.uid                              as user_id,
                               common.ch                               as channel,
                               common.ar                               as area_code,
                               common.vc                               as version_code,
                               common.mid                              as mid_id,
                               common.ba                               as brand,
                               common.md                               as model,
                               common.os                               as operate_system,
                               ts,
                               if(page.last_page_id is null, ts, null) as session_start_point
                        from {data_base}.ods_log_inc
                        where dt = '{start_date}' and page is not null
                    ) as t
                ) as u where user_id is not null
            ) as v where rn = 1
        ) as log left join
        (
            select id province_id,
                   area_code
            from {data_base}.ods_base_province_full
            where dt = '{start_date}'
        ) as base_province on log.area_code = base_province.area_code;"""

    ods2dwd_sqls = [dwd_trade_cart_add_inc, dwd_trade_order_detail_inc, dwd_trade_cancel_detail_inc,
                    dwd_trade_pay_detail_suc_inc, dwd_trade_order_refund_inc,
                    dwd_trade_refund_pay_suc_inc, dwd_trade_cart_full, dwd_tool_coupon_get_inc,
                    dwd_tool_coupon_order_inc, dwd_tool_coupon_pay_inc, dwd_interaction_favor_add_inc,
                    dwd_interaction_comment_inc, dwd_traffic_page_view_inc, dwd_traffic_start_inc,
                    dwd_traffic_action_inc, dwd_traffic_display_inc, dwd_traffic_error_inc,
                    dwd_user_register_inc, dwd_user_login_inc]
    return ods2dwd_sqls
