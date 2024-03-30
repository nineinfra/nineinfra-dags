def get_ods_daily2inc_sqls(data_base, start_date):
    ods_inc_cart_info = f"""
        INSERT OVERWRITE TABLE {data_base}.ods_cart_info_inc PARTITION (dt='{start_date}')
        SELECT
            CASE
                WHEN substr(create_time, 1, 10) = '{start_date}' THEN 'bootstrap-insert'
                WHEN substr(operate_time, 1, 10) = '{start_date}' THEN 'update'
                ELSE 'insert'
            END as type,
            UNIX_TIMESTAMP(operate_time) as ts,
            named_struct(
                'id', id,
                'user_id', user_id,
                'sku_id', sku_id,
                'cart_price', cart_price,
                'sku_num', sku_num,
                'img_url', img_url,
                'sku_name', sku_name,
                'is_checked', is_checked,
                'create_time', create_time,
                'operate_time', operate_time,
                'is_ordered', is_ordered,
                'order_time', order_time,
                'source_type', source_type,
                'source_id', source_id
            ) as data,
            map(
                'id', id,
                'user_id', user_id,
                'sku_id', sku_id,
                'cart_price', cart_price,
                'sku_num', sku_num,
                'img_url', img_url,
                'sku_name', sku_name,
                'is_checked', is_checked,
                'create_time', create_time,
                'operate_time', operate_time,
                'is_ordered', is_ordered,
                'order_time', order_time,
                'source_type', source_type,
                'source_id', source_id
            ) as old
        FROM {data_base}.ods_cart_info_daily
        WHERE (substr(create_time, 1, 10) = '{start_date}' OR substr(operate_time, 1, 10) = '{start_date}');
        """

    ods_inc_comment_info = f"""
        INSERT OVERWRITE TABLE {data_base}.ods_comment_info_inc PARTITION (dt='{start_date}')
        SELECT
            CASE
                WHEN substr(create_time, 1, 10) = '{start_date}' THEN 'bootstrap-insert'
                WHEN substr(operate_time, 1, 10) = '{start_date}' THEN 'update'
                ELSE 'insert'
            END as type,
            UNIX_TIMESTAMP(operate_time) as ts,
            named_struct(
                'id', id,
                'user_id', user_id,
                'nick_name', nick_name,
                'head_img', head_img,
                'sku_id', sku_id,
                'spu_id', spu_id,
                'order_id', order_id,
                'appraise', appraise,
                'comment_txt', comment_txt,
                'create_time', create_time,
                'operate_time', operate_time
            ) as data,
            map(
                'id', id,
                'user_id', user_id,
                'nick_name', nick_name,
                'head_img', head_img,
                'sku_id', sku_id,
                'spu_id', spu_id,
                'order_id', order_id,
                'appraise', appraise,
                'comment_txt', comment_txt,
                'create_time', create_time,
                'operate_time', operate_time
            ) as old
        FROM {data_base}.ods_comment_info_daily
        WHERE (substr(create_time, 1, 10) = '{start_date}' OR substr(operate_time, 1, 10) = '{start_date}');
        """

    ods_inc_coupon_use = f"""
        INSERT OVERWRITE TABLE {data_base}.ods_coupon_use_inc PARTITION (dt='{start_date}')
        SELECT
            CASE
                WHEN substr(get_time, 1, 10) = '{start_date}' THEN 'bootstrap-insert'
                WHEN substr(using_time, 1, 10) = '{start_date}' THEN 'update'
                WHEN substr(used_time, 1, 10) = '{start_date}' THEN 'update'
                ELSE 'insert'
            END as type,
            UNIX_TIMESTAMP(COALESCE(using_time, used_time, expire_time, get_time)) as ts,
            named_struct(
                'id', id,
                'coupon_id', coupon_id,
                'user_id', user_id,
                'order_id', order_id,
                'coupon_status', coupon_status,
                'get_time', get_time,
                'using_time', using_time,
                'used_time', used_time,
                'expire_time', expire_time
            ) as data,
            map(
                'id', id,
                'coupon_id', coupon_id,
                'user_id', user_id,
                'order_id', order_id,
                'coupon_status', coupon_status,
                'get_time', get_time,
                'using_time', using_time,
                'used_time', used_time,
                'expire_time', expire_time
            ) as old
        FROM {data_base}.ods_coupon_use_daily
        WHERE (substr(get_time, 1, 10) = '{start_date}' OR substr(using_time, 1, 10) = '{start_date}' OR substr(used_time, 1, 10) = '{start_date}' OR substr(expire_time, 1, 10) = '{start_date}');
        """

    ods_inc_favor_info = f"""
        INSERT OVERWRITE TABLE {data_base}.ods_favor_info_inc PARTITION (dt='{start_date}')
        SELECT 
            CASE 
                WHEN substr(create_time, 1, 10) = '{start_date}' THEN 'bootstrap-insert'
                WHEN substr(cancel_time, 1, 10) = '{start_date}' THEN 'update'
                ELSE 'insert'
            END as type,
            UNIX_TIMESTAMP(COALESCE(cancel_time, create_time)) as ts,
            named_struct(
                'id', id,
                'user_id', user_id,
                'sku_id', sku_id,
                'spu_id', spu_id,
                'is_cancel', is_cancel,
                'create_time', create_time,
                'cancel_time', cancel_time
            ) as data,
            map(
                'id', id,
                'user_id', user_id,
                'sku_id', sku_id,
                'spu_id', spu_id,
                'is_cancel', is_cancel,
                'create_time', create_time,
                'cancel_time', cancel_time
            ) as old
        FROM {data_base}.ods_favor_info_daily
        WHERE (substr(create_time, 1, 10) = '{start_date}' OR substr(cancel_time, 1, 10) = '{start_date}');
        """

    ods_inc_order_detail = f"""
        INSERT OVERWRITE TABLE {data_base}.ods_order_detail_inc PARTITION (dt='{start_date}')
        SELECT 
            CASE 
                WHEN substr(create_time, 1, 10) = '{start_date}' THEN 'bootstrap-insert'
                ELSE 'insert'
            END as type,
            UNIX_TIMESTAMP(create_time) as ts,
            named_struct(
                'id', id,
                'order_id', order_id,
                'sku_id', sku_id,
                'sku_name', sku_name,
                'img_url', img_url,
                'order_price', order_price,
                'sku_num', sku_num,
                'create_time', create_time,
                'source_type', source_type,
                'source_id', source_id,
                'split_total_amount', split_total_amount,
                'split_activity_amount', split_activity_amount,
                'split_coupon_amount', split_coupon_amount
            ) as data,
            map(
                'id', id,
                'order_id', order_id,
                'sku_id', sku_id,
                'sku_name', sku_name,
                'img_url', img_url,
                'order_price', order_price,
                'sku_num', sku_num,
                'create_time', create_time,
                'source_type', source_type,
                'source_id', source_id,
                'split_total_amount', split_total_amount,
                'split_activity_amount', split_activity_amount,
                'split_coupon_amount', split_coupon_amount
            ) as old
        FROM {data_base}.ods_order_detail_daily
        WHERE substr(create_time, 1, 10) = '{start_date}';
        """
    ods_inc_order_detail_activity = f"""
        INSERT OVERWRITE TABLE {data_base}.ods_order_detail_activity_inc PARTITION (dt='{start_date}')
        SELECT 
            CASE 
                WHEN substr(create_time, 1, 10) = '{start_date}' THEN 'bootstrap-insert'
                ELSE 'insert'
            END as type,
            UNIX_TIMESTAMP(create_time) as ts,
            named_struct(
                'id', id,
                'order_id', order_id,
                'order_detail_id', order_detail_id,
                'activity_id', activity_id,
                'activity_rule_id', activity_rule_id,
                'sku_id', sku_id,
                'create_time', create_time
            ) as data,
            map(
                'id', id,
                'order_id', order_id,
                'order_detail_id', order_detail_id,
                'activity_id', activity_id,
                'activity_rule_id', activity_rule_id,
                'sku_id', sku_id,
                'create_time', create_time
            ) as old
        FROM {data_base}.ods_order_detail_activity_daily
        WHERE substr(create_time, 1, 10) = '{start_date}';
        """

    ods_inc_order_detail_coupon = f"""
        INSERT OVERWRITE TABLE {data_base}.ods_order_detail_coupon_inc PARTITION (dt='{start_date}')
        SELECT 
            CASE 
                WHEN substr(create_time, 1, 10) = '{start_date}' THEN 'bootstrap-insert'
                ELSE 'insert'
            END as type,
            UNIX_TIMESTAMP(create_time) as ts,
            named_struct(
                'id', id,
                'order_id', order_id,
                'order_detail_id', order_detail_id,
                'coupon_id', coupon_id,
                'coupon_use_id', coupon_use_id,
                'sku_id', sku_id,
                'create_time', create_time
            ) as data,
            map(
                'id', id,
                'order_id', order_id,
                'order_detail_id', order_detail_id,
                'coupon_id', coupon_id,
                'coupon_use_id', coupon_use_id,
                'sku_id', sku_id,
                'create_time', create_time
            ) as old
        FROM {data_base}.ods_order_detail_coupon_daily
        WHERE substr(create_time, 1, 10) = '{start_date}';
        """

    ods_inc_order_info = f"""
        INSERT OVERWRITE TABLE {data_base}.ods_order_info_inc PARTITION (dt='{start_date}')
        SELECT 
            CASE 
                WHEN substr(create_time, 1, 10) = '{start_date}' THEN 'bootstrap-insert'
                WHEN substr(operate_time, 1, 10) = '{start_date}' THEN 'update'
                ELSE 'insert'
            END as type,
            UNIX_TIMESTAMP(create_time) as ts,
            named_struct(
                'id', id,
                'consignee', consignee,
                'consignee_tel', consignee_tel,
                'total_amount', total_amount,
                'order_status', order_status,
                'user_id', user_id,
                'payment_way', payment_way,
                'delivery_address', delivery_address,
                'order_comment', order_comment,
                'out_trade_no', out_trade_no,
                'trade_body', trade_body,
                'create_time', create_time,
                'operate_time', operate_time,
                'expire_time', expire_time,
                'process_status', process_status,
                'tracking_no', tracking_no,
                'parent_order_id', parent_order_id,
                'img_url', img_url,
                'province_id', province_id,
                'activity_reduce_amount', activity_reduce_amount,
                'coupon_reduce_amount', coupon_reduce_amount,
                'original_total_amount', original_total_amount,
                'freight_fee', freight_fee,
                'freight_fee_reduce', freight_fee_reduce,
                'refundable_time', refundable_time
            ) as data,
            map(
                'id', id,
                'consignee', consignee,
                'consignee_tel', consignee_tel,
                'total_amount', total_amount,
                'order_status', order_status,
                'user_id', user_id,
                'payment_way', payment_way,
                'delivery_address', delivery_address,
                'order_comment', order_comment,
                'out_trade_no', out_trade_no,
                'trade_body', trade_body,
                'create_time', create_time,
                'operate_time', operate_time,
                'expire_time', expire_time,
                'process_status', process_status,
                'tracking_no', tracking_no,
                'parent_order_id', parent_order_id,
                'img_url', img_url,
                'province_id', province_id,
                'activity_reduce_amount', activity_reduce_amount,
                'coupon_reduce_amount', coupon_reduce_amount,
                'original_total_amount', original_total_amount,
                'freight_fee', freight_fee,
                'freight_fee_reduce', freight_fee_reduce,
                'refundable_time', refundable_time
            ) as old
        FROM {data_base}.ods_order_info_daily
        WHERE substr(create_time, 1, 10) = '{start_date}' OR substr(operate_time, 1, 10) = '{start_date}';
        """

    ods_inc_order_refund_info = f"""
        INSERT OVERWRITE TABLE {data_base}.ods_order_refund_info_inc PARTITION (dt='{start_date}')
        SELECT 
            CASE 
                WHEN substr(create_time, 1, 10) = '{start_date}' THEN 'bootstrap-insert'
                ELSE 'insert'
            END as type,
            UNIX_TIMESTAMP(create_time) as ts,
            named_struct(
                'id', id,
                'user_id', user_id,
                'order_id', order_id,
                'sku_id', sku_id,
                'refund_type', refund_type,
                'refund_num', refund_num,
                'refund_amount', refund_amount,
                'refund_reason_type', refund_reason_type,
                'refund_reason_txt', refund_reason_txt,
                'refund_status', refund_status,
                'create_time', create_time
            ) as data,
            map(
                'id', id,
                'user_id', user_id,
                'order_id', order_id,
                'sku_id', sku_id,
                'refund_type', refund_type,
                'refund_num', refund_num,
                'refund_amount', refund_amount,
                'refund_reason_type', refund_reason_type,
                'refund_reason_txt', refund_reason_txt,
                'refund_status', refund_status,
                'create_time', create_time
            ) as old
        FROM {data_base}.ods_order_refund_info_daily
        WHERE substr(create_time, 1, 10) = '{start_date}';
        """

    ods_inc_order_status_log = f"""
        INSERT OVERWRITE TABLE {data_base}.ods_order_status_log_inc PARTITION (dt='{start_date}')
        SELECT 
            CASE 
                WHEN substr(operate_time, 1, 10) = '{start_date}' THEN 'insert'
                ELSE 'insert'
            END as type,
            UNIX_TIMESTAMP(operate_time) as ts,
            named_struct(
                'id', id,
                'order_id', order_id,
                'order_status', order_status,
                'operate_time', operate_time
            ) as data,
            map(
                'id', id,
                'order_id', order_id,
                'order_status', order_status,
                'operate_time', operate_time
            ) as old
        FROM {data_base}.ods_order_status_log_daily
        WHERE substr(operate_time, 1, 10) = '{start_date}';
        """

    ods_inc_payment_info = f"""
        INSERT OVERWRITE TABLE {data_base}.ods_payment_info_inc PARTITION (dt='{start_date}')
        SELECT 
            CASE 
                WHEN substr(create_time, 1, 10) = '{start_date}' THEN 'bootstrap-insert'
                WHEN substr(callback_time, 1, 10) = '{start_date}' THEN 'update'
                ELSE 'insert'
            END as type,
            UNIX_TIMESTAMP(create_time) as ts,
            named_struct(
                'id', id,
                'out_trade_no', out_trade_no,
                'order_id', order_id,
                'user_id', user_id,
                'payment_type', payment_type,
                'trade_no', trade_no,
                'total_amount', total_amount,
                'subject', subject,
                'payment_status', payment_status,
                'create_time', create_time,
                'callback_time', callback_time,
                'callback_content', callback_content
            ) as data,
            map(
                'id', id,
                'out_trade_no', out_trade_no,
                'order_id', order_id,
                'user_id', user_id,
                'payment_type', payment_type,
                'trade_no', trade_no,
                'total_amount', total_amount,
                'subject', subject,
                'payment_status', payment_status,
                'create_time', create_time,
                'callback_time', callback_time,
                'callback_content', callback_content
            ) as old
        FROM {data_base}.ods_payment_info_daily
        WHERE substr(create_time, 1, 10) = '{start_date}' OR substr(callback_time, 1, 10) = '{start_date}';
        """

    ods_inc_refund_payment = f"""
        INSERT OVERWRITE TABLE {data_base}.ods_refund_payment_inc PARTITION (dt='{start_date}')
        SELECT 
            CASE 
                WHEN substr(create_time, 1, 10) = '{start_date}' THEN 'bootstrap-insert'
                WHEN substr(callback_time, 1, 10) = '{start_date}' THEN 'update'
                ELSE 'insert'
            END as type,
            UNIX_TIMESTAMP(create_time) as ts,
            named_struct(
                'id', id,
                'out_trade_no', out_trade_no,
                'order_id', order_id,
                'sku_id', sku_id,
                'payment_type', payment_type,
                'trade_no', trade_no,
                'total_amount', total_amount,
                'subject', subject,
                'refund_status', refund_status,
                'create_time', create_time,
                'callback_time', callback_time,
                'callback_content', callback_content
            ) as data,
            map(
                'id', id,
                'out_trade_no', out_trade_no,
                'order_id', order_id,
                'sku_id', sku_id,
                'payment_type', payment_type,
                'trade_no', trade_no,
                'total_amount', total_amount,
                'subject', subject,
                'refund_status', refund_status,
                'create_time', create_time,
                'callback_time', callback_time,
                'callback_content', callback_content
            ) as old
        FROM {data_base}.ods_refund_payment_daily
        WHERE substr(create_time, 1, 10) = '{start_date}' OR substr(callback_time, 1, 10) = '{start_date}';
        """

    ods_inc_user_info = f"""
        INSERT OVERWRITE TABLE {data_base}.ods_user_info_inc PARTITION (dt='{start_date}')
        SELECT 
            CASE 
                WHEN substr(create_time, 1, 10) = '{start_date}' THEN 'bootstrap-insert'
                WHEN substr(operate_time, 1, 10) = '{start_date}' THEN 'update'
                ELSE 'insert'
            END as type,
            UNIX_TIMESTAMP(operate_time) as ts,
            named_struct(
                'id', id,
                'login_name', login_name,
                'nick_name', nick_name,
                'passwd', passwd,
                'name', name,
                'phone_num', phone_num,
                'email', email,
                'head_img', head_img,
                'user_level', user_level,
                'birthday', birthday,
                'gender', gender,
                'create_time', create_time,
                'operate_time', operate_time,
                'status', status
            ) as data,
            map(
                'id', id,
                'login_name', login_name,
                'nick_name', nick_name,
                'passwd', passwd,
                'name', name,
                'phone_num', phone_num,
                'email', email,
                'head_img', head_img,
                'user_level', user_level,
                'birthday', birthday,
                'gender', gender,
                'create_time', create_time,
                'operate_time', operate_time,
                'status', status
            ) as old
        FROM {data_base}.ods_user_info_daily
        WHERE substr(create_time, 1, 10) = '{start_date}' OR substr(operate_time, 1, 10) = '{start_date}';
        """

    ods_daily2inc_sqls = [ods_inc_cart_info, ods_inc_comment_info, ods_inc_coupon_use, ods_inc_favor_info,
                          ods_inc_order_detail, ods_inc_order_detail_activity, ods_inc_order_detail_coupon,
                          ods_inc_order_info, ods_inc_order_refund_info, ods_inc_order_status_log,
                          ods_inc_payment_info, ods_inc_refund_payment, ods_inc_user_info]
    return ods_daily2inc_sqls
