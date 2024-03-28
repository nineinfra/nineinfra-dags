DATA_BASE = "datahouse"
start_date = "2024-03-27"

ads_traffic_stats_by_channel = f"\n\
    insert overwrite table {DATA_BASE}.ads_traffic_stats_by_channel\n\
    select dt, recent_days, channel, uv_count, avg_duration_sec, avg_page_count, sv_count, bounce_rate\n\
    from ads_traffic_stats_by_channel as traffic_stats\n\
    union\n\
    select '{start_date}'                                                                               as dt,\n\
           recent_days,\n\
           channel,\n\
           cast(count(distinct (mid_id)) as bigint)                                                   as uv_count,\n\
           cast(avg(during_time_1d) / 1000 as bigint)                                                 as avg_duration_sec,\n\
           cast(avg(page_count_1d) as bigint)                                                         as avg_page_count,\n\
           cast(count(*) as bigint)                                                                   as sv_count,\n\
           cast(sum(if(page_count_1d = 1, 1, 0)) / count(*) as decimal(16, 2))                        as bounce_rate\n\
    from {DATA_BASE}.dws_traffic_session_page_view_1d lateral view explode(array(1, 7, 30)) tmp as recent_days\n\
    where dt >= date_add('{start_date}', -recent_days + 1)\n\
    group by recent_days, channel;"

ads_page_path = f"\n\
    insert overwrite table {DATA_BASE}.ads_page_path\n\
    select dt, recent_days, source, target, path_count\n\
    from ads_page_path as page_path\n\
    union\n\
    select '{start_date}'                  as dt,\n\
           page_view.recent_days,\n\
           page_view.source,\n\
           nvl(page_view.target, 'null') as target,\n\
           count(*)                      as path_count\n\
    from\n\
    (\n\
        select page.recent_days,\n\
               concat('step-', page.rn,     ':', page.page_id)      as source,\n\
               concat('step-', page.rn + 1, ':', page.next_page_id) as target\n\
        from\n\
        (\n\
            select recent_days,\n\
                   page_id,\n\
                   lead(page_id, 1, null) over (partition by session_id, recent_days)                    as next_page_id,\n\
                   row_number()           over (partition by session_id, recent_days order by view_time) as rn\n\
            from {DATA_BASE}.dwd__traffic_page_view_inc lateral view explode(array(1, 7, 30)) tmp  as recent_days\n\
            where dt >= date_add('{start_date}', -recent_days + 1)\n\
        ) as page\n\
    ) as page_view\n\
    group by page_view.recent_days, page_view.source, page_view.target;"

ads_user_change = f"\n\
    insert overwrite table {DATA_BASE}.ads_user_change\n\
    select dt, user_churn_count, user_back_count\n\
    from ads_user_change as user_change\n\
    union\n\
    select user_login.dt,\n\
           user_login.user_churn_count,\n\
           back.user_back_count\n\
    from\n\
    (\n\
        select '{start_date}' as dt,\n\
               count(*)     as user_churn_count\n\
        from {DATA_BASE}.dws_user_user_login_td\n\
        where dt = '{start_date}' and login_date_last = date_add('{start_date}', -7)\n\
    ) as user_login join\n\
    (\n\
        select '{start_date}' as dt,\n\
               count(*)     as user_back_count\n\
        from\n\
        (\n\
            select user_id,\n\
                   login_date_last\n\
            from {DATA_BASE}.dws_user_user_login_td\n\
            where dt = '{start_date}' and login_date_last = '{start_date}'         -- 今日活跃的用户\n\
        ) as login_1 join\n\
        (\n\
            select user_id,\n\
                   login_date_last as login_date_previous\n\
            from {DATA_BASE}.dws_user_user_login_td\n\
            where dt = date_add('{start_date}', -1)                              -- 找出今日活跃用户的上次活跃日期\n\
        ) as login_2 on login_1.user_id = login_2.user_id\n\
        where datediff(login_1.login_date_last, login_2.login_date_previous) >= 8\n\
    ) as back on user_login.dt = back.dt;"

ads_user_retention = f"\n\
    insert overwrite table {DATA_BASE}.ads_user_retention\n\
    select dt, create_date, retention_day, retention_count, new_user_count, retention_rate\n\
    from ads_user_retention as user_retention\n\
    union\n\
    select '{start_date}'                                                                                 as dt,\n\
           register.login_date_first                                                                    as create_date,\n\
           datediff('{start_date}', register.login_date_first)                                            as retention_day,\n\
           sum(if(login.login_date_last = '{start_date}', 1, 0))                                          as retention_count,\n\
           count(*)                                                                                     as new_user_count,\n\
           cast(sum(if(login.login_date_last = '{start_date}', 1, 0)) / count(*) * 100 as decimal(16, 2)) as retention_rate\n\
    from\n\
    (\n\
        select user_id,\n\
               date_id  as login_date_first\n\
        from {DATA_BASE}.dwd__user_register_inc\n\
        where dt >= date_add('{start_date}', -7) and dt < '{start_date}'\n\
    ) as register join\n\
    (\n\
        select user_id,\n\
               login_date_last\n\
        from {DATA_BASE}.dws_user_user_login_td\n\
        where dt = '{start_date}'\n\
    ) as login on register.user_id = login.user_id\n\
    group by register.login_date_first;"

ads_user_stats = f"\n\
    insert overwrite table {DATA_BASE}.ads_user_stats\n\
    select dt, recent_days, new_user_count, active_user_count\n\
    from ads_user_stats as stats\n\
    union\n\
    select '{start_date}'             as dt,\n\
           register.recent_days,\n\
           register.new_user_count,\n\
           login.active_user_count\n\
    from\n\
    (\n\
        select recent_days,\n\
               count(*)     as new_user_count\n\
        from {DATA_BASE}.dwd__user_register_inc lateral view explode(array(1, 7, 30)) tmp as recent_days\n\
        where dt >= date_add('{start_date}', - recent_days + 1)\n\
        group by recent_days\n\
    ) as register join\n\
    (\n\
        select recent_days,\n\
               count(*)    as active_user_count\n\
        from {DATA_BASE}.dws_user_user_login_td lateral view explode(array(1, 7, 30)) tmp as recent_days\n\
        where dt = '{start_date}' and login_date_last >= date_add('{start_date}', - recent_days + 1)\n\
        group by recent_days\n\
    ) as login on register.recent_days = login.recent_days;"

ads_user_action = f"\n\
    insert overwrite table {DATA_BASE}.ads_user_action\n\
    select dt, recent_days, home_count, good_detail_count, cart_count, order_count, payment_count\n\
    from ads_user_action as action\n\
    union\n\
    select '{start_date}'            as dt,\n\
           page.recent_days,\n\
           page.home_count,\n\
           page.good_detail_count,\n\
           cart.cart_count,\n\
           user_order.order_count,\n\
           pay.payment_count\n\
    from\n\
    (\n\
        select 1                                      as recent_days,\n\
               sum(if(page_id = 'home',        1, 0)) as home_count,\n\
               sum(if(page_id = 'good_detail', 1, 0)) as good_detail_count\n\
        from {DATA_BASE}.dws_traffic_page_visitor_page_view_1d\n\
        where dt = '{start_date}' and page_id in ('home', 'good_detail')\n\
        union all\n\
        select visitor_view.recent_days,\n\
               sum(if(visitor_view.page_id = 'home'        and visitor_view.view_count > 0, 1, 0)) as home_count,\n\
               sum(if(visitor_view.page_id = 'good_detail' and visitor_view.view_count > 0, 1, 0)) as good_detail_count\n\
        from\n\
        (\n\
            select recent_days,\n\
                   page_id,\n\
                   case recent_days\n\
                       when 7 then  view_count_7d\n\
                       when 30 then view_count_30d\n\
                    end                                                                       as view_count\n\
            from {DATA_BASE}.dws_traffic_page_visitor_page_view_nd lateral view explode(array(7, 30)) tmp as recent_days\n\
            where dt = '{start_date}' and page_id in ('home', 'good_detail')\n\
        ) as visitor_view group by visitor_view.recent_days\n\
    ) as page join\n\
    (\n\
        select 1        as recent_days,\n\
               count(*) as cart_count\n\
        from {DATA_BASE}.dws_trade_user_cart_add_1d\n\
        where dt = '{start_date}'\n\
        union all\n\
        select user_cart.recent_days,\n\
               sum(if(user_cart.cart_count > 0, 1, 0)) as cart_count\n\
        from\n\
        (\n\
            select recent_days,\n\
                   case recent_days\n\
                       when 7  then cart_add_count_7d\n\
                       when 30 then cart_add_count_30d\n\
                   end                                                                               as cart_count\n\
            from {DATA_BASE}.dws_trade_user_cart_add_nd lateral view explode(array(7, 30)) tmp as recent_days\n\
            where dt = '{start_date}'\n\
        ) as user_cart group by recent_days\n\
    ) as cart\n\
        on page.recent_days = cart.recent_days\n\
    join\n\
    (\n\
        select 1        as recent_days,\n\
               count(*) as order_count\n\
        from {DATA_BASE}.dws_trade_user_order_1d\n\
        where dt = '{start_date}'\n\
        union all\n\
        select trade_user_order.recent_days,\n\
               sum(if(trade_user_order.order_count > 0, 1, 0)) as order_count\n\
        from\n\
        (\n\
            select recent_days,\n\
                   case recent_days\n\
                       when 7 then order_count_7d\n\
                       when 30 then order_count_30d\n\
                   end                                                                            as order_count\n\
            from {DATA_BASE}.dws_trade_user_order_nd lateral view explode(array(7, 30)) tmp as recent_days\n\
            where dt = '{start_date}'\n\
        ) as trade_user_order group by trade_user_order.recent_days\n\
    ) as user_order\n\
        on page.recent_days = user_order.recent_days\n\
    join\n\
    (\n\
        select 1         as recent_days,\n\
               count(*)  as payment_count\n\
        from {DATA_BASE}.dws_trade_user_payment_1d\n\
        where dt = '{start_date}'\n\
        union all\n\
        select user_payment.recent_days,\n\
               sum(if(user_payment.order_count > 0, 1, 0)) as payment_count\n\
        from\n\
        (\n\
            select recent_days,\n\
                   case recent_days\n\
                       when 7  then payment_count_7d\n\
                       when 30 then payment_count_30d\n\
                   end                                                                              as order_count\n\
            from {DATA_BASE}.dws_trade_user_payment_nd lateral view explode(array(7, 30)) tmp as recent_days\n\
            where dt = '{start_date}'\n\
        ) as user_payment group by user_payment.recent_days\n\
    ) as pay on page.recent_days = pay.recent_days;"

ads_new_buyer_stats = f"\n\
    insert overwrite table {DATA_BASE}.ads_new_buyer_stats\n\
    select dt, recent_days, new_order_user_count, new_payment_user_count\n\
    from ads_new_buyer_stats as buyer_stats\n\
    union\n\
    select '{start_date}'                     as dt,\n\
           user_order.recent_days,\n\
           user_order.new_order_user_count,\n\
           pay.new_payment_user_count\n\
    from\n\
    (\n\
        select recent_days,\n\
               sum(if(order_date_first >= date_add('{start_date}', -recent_days + 1), 1, 0))       as new_order_user_count\n\
        from {DATA_BASE}.dws_trade_user_order_td lateral view explode(array(1, 7, 30)) tmp as recent_days\n\
        where dt = '{start_date}'\n\
        group by recent_days\n\
    ) as user_order join\n\
    (\n\
        select recent_days,\n\
               sum(if(payment_date_first >= date_add('{start_date}', -recent_days + 1), 1, 0))       as new_payment_user_count\n\
        from {DATA_BASE}.dws_trade_user_payment_td lateral view explode(array(1, 7, 30)) tmp as recent_days\n\
        where dt = '{start_date}'\n\
        group by recent_days\n\
    ) pay on user_order.recent_days = pay.recent_days;"

ads_repeat_purchase_by_tm = f"\n\
    insert overwrite table {DATA_BASE}.ads_repeat_purchase_by_tm\n\
    select dt, recent_days, tm_id, tm_name, order_repeat_rate\n\
    from ads_repeat_purchase_by_tm as repeat_purchase\n\
    union\n\
    select '{start_date}'                                                                   as dt,\n\
           sku.recent_days,\n\
           sku.tm_id,\n\
           sku.tm_name,\n\
           cast(sum(if(sku.order_count >= 2, 1, 0)) / sum(if(sku.order_count >= 1, 1, 0)) as decimal(16, 2))\n\
    from\n\
    (\n\
        select user_sku_order.recent_days,\n\
               user_sku_order.tm_id,\n\
               user_sku_order.tm_name,\n\
               sum(user_sku_order.order_count) as order_count\n\
        from\n\
        (\n\
            select recent_days,\n\
                   user_id,\n\
                   tm_id,\n\
                   tm_name,\n\
                   case recent_days\n\
                       when 7  then order_count_7d\n\
                       when 30 then order_count_30d\n\
                   end                                                                                as order_count\n\
            from {DATA_BASE}.dws_trade_user_sku_order_nd lateral view explode(array(7, 30)) tmp as recent_days\n\
            where dt = '{start_date}'\n\
        ) as user_sku_order\n\
        group by user_sku_order.recent_days, user_sku_order.user_id, user_sku_order.tm_id, user_sku_order.tm_name\n\
    ) as sku\n\
    group by sku.recent_days, sku.tm_id, sku.tm_name;"

ads_trade_stats_by_tm = f"\n\
    insert overwrite table {DATA_BASE}.ads_trade_stats_by_tm\n\
    select dt, recent_days, tm_id, tm_name, order_count, order_user_count, order_refund_count, order_refund_user_count\n\
    from ads_trade_stats_by_tm as trade_stats\n\
    union\n\
    select '{start_date}'                                            as dt,\n\
           nvl(sku.recent_days,                refund.recent_days) as recent_days,\n\
           nvl(sku.tm_id,                      refund.tm_id)       as tm_id,\n\
           nvl(sku.tm_name,                    refund.tm_name)     as tm_name,\n\
           nvl(sku.order_count,                0)                  as order_count,\n\
           nvl(sku.order_user_count,           0)                  as order_user_count,\n\
           nvl(refund.order_refund_count,      0)                  as order_refund_count,\n\
           nvl(refund.order_refund_user_count, 0)                  as order_refund_user_count\n\
    from\n\
    (\n\
        select 1                         as recent_days,\n\
               tm_id,\n\
               tm_name,\n\
               sum(order_count_1d)       as order_count,\n\
               count(distinct (user_id)) as order_user_count\n\
        from {DATA_BASE}.dws_trade_user_sku_order_1d\n\
        where dt = '{start_date}'\n\
        group by tm_id, tm_name\n\
        union all\n\
        select user_sku_order.recent_days,\n\
               user_sku_order.tm_id,\n\
               user_sku_order.tm_name,\n\
               sum(user_sku_order.order_count)                                                    as order_count,\n\
               count(distinct (if(user_sku_order.order_count > 0, user_sku_order.user_id, null))) as order_user_count\n\
        from\n\
        (\n\
            select recent_days,\n\
                   user_id,\n\
                   tm_id,\n\
                   tm_name,\n\
                   case recent_days\n\
                       when 7 then  order_count_7d\n\
                       when 30 then order_count_30d\n\
                   end                                                                                as order_count\n\
            from {DATA_BASE}.dws_trade_user_sku_order_nd lateral view explode(array(7, 30)) tmp as recent_days\n\
            where dt = '{start_date}'\n\
        ) as user_sku_order\n\
        group by user_sku_order.recent_days, user_sku_order.tm_id, user_sku_order.tm_name\n\
    ) as sku full outer join\n\
    (\n\
        select 1                          as recent_days,\n\
               tm_id,\n\
               tm_name,\n\
               sum(order_refund_count_1d) as order_refund_count,\n\
               count(distinct (user_id))  as order_refund_user_count\n\
        from {DATA_BASE}.dws_trade_user_sku_order_refund_1d\n\
        where dt = '{start_date}'\n\
        group by tm_id, tm_name\n\
        union all\n\
        select order_refund.recent_days,\n\
               order_refund.tm_id,\n\
               order_refund.tm_name,\n\
               sum(order_refund.order_refund_count)                          as order_refund_count,\n\
               count(if(order_refund.order_refund_count > 0, user_id, null)) as order_refund_user_count\n\
        from\n\
        (\n\
            select recent_days,\n\
                   user_id,\n\
                   tm_id,\n\
                   tm_name,\n\
                   case recent_days\n\
                       when 7  then order_refund_count_7d\n\
                       when 30 then order_refund_count_30d\n\
                   end                                                                                       as order_refund_count\n\
            from {DATA_BASE}.dws_trade_user_sku_order_refund_nd lateral view explode(array(7, 30)) tmp as recent_days\n\
            where dt = '{start_date}'\n\
        ) as order_refund\n\
        group by order_refund.recent_days, order_refund.tm_id, order_refund.tm_name\n\
    ) as refund\n\
        on sku.recent_days  = refund.recent_days\n\
            and sku.tm_id   = refund.tm_id\n\
            and sku.tm_name = refund.tm_name;"

ads_trade_stats_by_cate = f"\n\
    insert overwrite table {DATA_BASE}.ads_trade_stats_by_cate\n\
    select dt, recent_days, category1_id, category1_name, category2_id, category2_name, category3_id, category3_name,\n\
           order_count, order_user_count, order_refund_count, order_refund_user_count\n\
    from ads_trade_stats_by_cate as trade_stats\n\
    union\n\
    select '{start_date}'                                               as dt,\n\
           nvl(odr.recent_days,                refund.recent_days)    as recent_days,\n\
           nvl(odr.category1_id,               refund.category1_id)   as category1_id,\n\
           nvl(odr.category1_name,             refund.category1_name) as category1_name,\n\
           nvl(odr.category2_id,               refund.category2_id)   as category2_id,\n\
           nvl(odr.category2_name,             refund.category2_name) as category2_name,\n\
           nvl(odr.category3_id,               refund.category3_id)   as category3_id,\n\
           nvl(odr.category3_name,             refund.category3_name) as category3_name,\n\
           nvl(odr.order_count,                0)                     as order_count,\n\
           nvl(odr.order_user_count,           0)                     as order_user_count,\n\
           nvl(refund.order_refund_count,      0)                     as order_refund_count,\n\
           nvl(refund.order_refund_user_count, 0)                     as order_refund_user_count\n\
    from\n\
    (\n\
        select 1                         as recent_days,\n\
               category1_id,\n\
               category1_name,\n\
               category2_id,\n\
               category2_name,\n\
               category3_id,\n\
               category3_name,\n\
               sum(order_count_1d)       as order_count,\n\
               count(distinct (user_id)) as order_user_count\n\
        from {DATA_BASE}.dws_trade_user_sku_order_1d\n\
        where dt = '{start_date}'\n\
        group by category1_id, category1_name, category2_id, category2_name, category3_id, category3_name\n\
        union all\n\
        select user_sku_order.recent_days,\n\
               user_sku_order.category1_id,\n\
               user_sku_order.category1_name,\n\
               user_sku_order.category2_id,\n\
               user_sku_order.category2_name,\n\
               user_sku_order.category3_id,\n\
               user_sku_order.category3_name,\n\
               sum(user_sku_order.order_count)                                                     as order_count,\n\
               count(distinct (if(user_sku_order.order_count > 0, user_sku_order.user_id, null)))  as order_user_count\n\
        from\n\
        (\n\
            select recent_days,\n\
                   user_id,\n\
                   category1_id,\n\
                   category1_name,\n\
                   category2_id,\n\
                   category2_name,\n\
                   category3_id,\n\
                   category3_name,\n\
                   case recent_days\n\
                       when 7  then order_count_7d\n\
                       when 30 then order_count_30d\n\
                   end                                                                                as order_count\n\
            from {DATA_BASE}.dws_trade_user_sku_order_nd lateral view explode(array(7, 30)) tmp as recent_days\n\
            where dt = '{start_date}'\n\
        ) as user_sku_order\n\
        group by user_sku_order.recent_days,   user_sku_order.category1_id,   user_sku_order.category1_name,\n\
                 user_sku_order.category2_id,  user_sku_order.category2_name, user_sku_order.category3_id,\n\
                 user_sku_order.category3_name\n\
    ) as odr full outer join\n\
    (\n\
        select 1                          as recent_days,\n\
               category1_id,\n\
               category1_name,\n\
               category2_id,\n\
               category2_name,\n\
               category3_id,\n\
               category3_name,\n\
               sum(order_refund_count_1d) as order_refund_count,\n\
               count(distinct (user_id))  as order_refund_user_count\n\
        from {DATA_BASE}.dws_trade_user_sku_order_refund_1d\n\
        where dt = '{start_date}'\n\
        group by category1_id, category1_name, category2_id, category2_name, category3_id, category3_name\n\
        union all\n\
        select sku_order_refund.recent_days,\n\
               sku_order_refund.category1_id,\n\
               sku_order_refund.category1_name,\n\
               sku_order_refund.category2_id,\n\
               sku_order_refund.category2_name,\n\
               sku_order_refund.category3_id,\n\
               sku_order_refund.category3_name,\n\
               sum(sku_order_refund.order_refund_count)                                                      as order_refund_count,\n\
               count(distinct (if(sku_order_refund.order_refund_count > 0, sku_order_refund.user_id, null))) as order_refund_user_count\n\
        from\n\
        (\n\
            select recent_days,\n\
                   user_id,\n\
                   category1_id,\n\
                   category1_name,\n\
                   category2_id,\n\
                   category2_name,\n\
                   category3_id,\n\
                   category3_name,\n\
                   case recent_days\n\
                       when 7  then order_refund_count_7d\n\
                       when 30 then order_refund_count_30d\n\
                   end                                                                     as order_refund_count\n\
            from {DATA_BASE}.dws_trade_user_sku_order_refund_nd lateral view explode(array(7, 30)) tmp as recent_days\n\
            where dt = '{start_date}'\n\
        ) as sku_order_refund\n\
        group by sku_order_refund.recent_days,  sku_order_refund.category1_id,   sku_order_refund.category1_name,\n\
                 sku_order_refund.category2_id, sku_order_refund.category2_name, sku_order_refund.category3_id,\n\
                 sku_order_refund.category3_name\n\
    ) as refund on odr.recent_days    = refund.recent_days\n\
               and odr.category1_id   = refund.category1_id\n\
               and odr.category1_name = refund.category1_name\n\
               and odr.category2_id   = refund.category2_id\n\
               and odr.category2_name = refund.category2_name\n\
               and odr.category3_id   = refund.category3_id\n\
               and odr.category3_name = refund.category3_name;"

ads_sku_cart_num_top3_by_cate = f"\n\
    insert overwrite table {DATA_BASE}.ads_sku_cart_num_top3_by_cate\n\
    select dt, category1_id, category1_name, category2_id, category2_name, category3_id, category3_name, sku_id, sku_name, cart_num, rk\n\
    from ads_sku_cart_num_top3_by_cate as sku_cart_num\n\
    union\n\
    select '{start_date}'                   as dt,\n\
           cart_num_top.category1_id,\n\
           cart_num_top.category1_name,\n\
           cart_num_top.category2_id,\n\
           cart_num_top.category2_name,\n\
           cart_num_top.category3_id,\n\
           cart_num_top.category3_name,\n\
           cart_num_top.sku_id,\n\
           cart_num_top.sku_name,\n\
           cart_num_top.cart_num,\n\
           cart_num_top.rk\n\
    from\n\
    (\n\
        select cart.sku_id,\n\
               sku.sku_name,\n\
               sku.category1_id,\n\
               sku.category1_name,\n\
               sku.category2_id,\n\
               sku.category2_name,\n\
               sku.category3_id,\n\
               sku.category3_name,\n\
               cart.cart_num,\n\
               rank() over (partition by sku.category1_id, sku.category2_id, sku.category3_id order by cart.cart_num desc) as rk\n\
        from\n\
        (\n\
            select sku_id,\n\
                   sum(sku_num)  as cart_num\n\
            from {DATA_BASE}.dwd__trade_cart_full\n\
            where dt = '{start_date}'\n\
            group by sku_id\n\
        ) as cart left join\n\
        (\n\
            select id,\n\
                   sku_name,\n\
                   category1_id,\n\
                   category1_name,\n\
                   category2_id,\n\
                   category2_name,\n\
                   category3_id,\n\
                   category3_name\n\
            from {DATA_BASE}.dim__sku_full\n\
            where dt = '{start_date}'\n\
        ) as sku on cart.sku_id = sku.id\n\
    ) as cart_num_top\n\
    where cart_num_top.rk <= 3;"

ads_trade_stats = f"\n\
    insert overwrite table {DATA_BASE}.ads_trade_stats\n\
    select dt, recent_days, order_total_amount, order_count, order_user_count, order_refund_count, order_refund_user_count\n\
    from ads_trade_stats as trade_stats\n\
    union\n\
    select '{start_date}'                      as dt,\n\
           odr.recent_days,\n\
           odr.order_total_amount,\n\
           odr.order_count,\n\
           odr.order_user_count,\n\
           refund.order_refund_count,\n\
           refund.order_refund_user_count\n\
    from\n\
    (\n\
        select 1                          as recent_days,\n\
               sum(order_total_amount_1d) as order_total_amount,\n\
               sum(order_count_1d)        as order_count,\n\
               count(*)                   as order_user_count\n\
        from {DATA_BASE}.dws_trade_user_order_1d\n\
        where dt = '{start_date}'\n\
        union all\n\
        select user_order.recent_days,\n\
               sum(user_order.order_total_amount)        as order_total_amount,\n\
               sum(user_order.order_count)               as order_count,\n\
               sum(if(user_order.order_count > 0, 1, 0)) as order_user_count\n\
        from\n\
        (\n\
            select recent_days,\n\
                  case recent_days\n\
                      when 7  then order_total_amount_7d\n\
                      when 30 then order_total_amount_30d\n\
                  end                                                                             as order_total_amount,\n\
                  case recent_days\n\
                      when 7  then order_count_7d\n\
                      when 30 then order_count_30d\n\
                  end                                                                             as order_count\n\
            from {DATA_BASE}.dws_trade_user_order_nd lateral view explode(array(7, 30)) tmp as recent_days\n\
            where dt = '{start_date}'\n\
        ) as user_order group by user_order.recent_days\n\
    ) as odr join\n\
    (\n\
        select 1                          as recent_days,\n\
               sum(order_refund_count_1d) as order_refund_count,\n\
               count(*)                   as order_refund_user_count\n\
        from {DATA_BASE}.dws_trade_user_order_refund_1d\n\
        where dt = '{start_date}'\n\
        union all\n\
        select order_refund.recent_days,\n\
               sum(order_refund.order_refund_count)               as order_refund_count,\n\
               sum(if(order_refund.order_refund_count > 0, 1, 0)) as order_refund_user_count\n\
        from\n\
        (\n\
            select recent_days,\n\
                   case recent_days\n\
                       when 7  then order_refund_count_7d\n\
                       when 30 then order_refund_count_30d\n\
                   end                                                                                   as order_refund_count\n\
            from {DATA_BASE}.dws_trade_user_order_refund_nd lateral view explode(array(7, 30)) tmp as recent_days\n\
            where dt = '{start_date}'\n\
        ) as order_refund group by order_refund.recent_days\n\
    ) as refund on odr.recent_days = refund.recent_days;"

ads_order_by_province = f"\n\
    insert overwrite table {DATA_BASE}.ads_order_by_province\n\
    select dt, recent_days, province_id, province_name, area_code, iso_code, iso_code_3166_2, order_count, order_total_amount\n\
    from ads_order_by_province  as order_province\n\
    union\n\
    select '{start_date}'             as dt,\n\
           1                        as recent_days,\n\
           province_id,\n\
           province_name,\n\
           area_code,\n\
           iso_code,\n\
           iso_3166_2,\n\
           order_count_1d           as order_count,\n\
           order_total_amount_1d    as order_total_amount\n\
    from {DATA_BASE}.dws_trade_province_order_1d\n\
    where dt = '{start_date}'\n\
    union all\n\
    select '{start_date}'                                                                       as dt,\n\
           recent_days,\n\
           province_id,\n\
           province_name,\n\
           area_code,\n\
           iso_code,\n\
           iso_3166_2,\n\
           if(recent_days = 7, order_count_7d,        order_count_30d)                        as order_count,\n\
           if(recent_days = 7, order_total_amount_7d, order_total_amount_30d)                 as order_total_amount\n\
    from {DATA_BASE}.dws_trade_province_order_nd lateral view explode(array(7, 30)) tmp as recent_days\n\
    where dt = '{start_date}';"

ads_coupon_stats = f"\n\
    insert overwrite table {DATA_BASE}.ads_coupon_stats\n\
    select dt, coupon_id, coupon_name, start_date, rule_name, reduce_rate\n\
    from ads_coupon_stats as coupon_stats\n\
    union\n\
    select '{start_date}'                                                           as dt,\n\
           coupon_id,\n\
           coupon_name,\n\
           start_date,\n\
           coupon_rule                                                            as rule_name,\n\
           cast(coupon_reduce_amount_30d / original_amount_30d as decimal(16, 2)) as reduce_rate\n\
    from {DATA_BASE}.dws_trade_coupon_order_nd\n\
    where dt = '{start_date}';"

ads_activity_stats = f"\n\
    insert overwrite table {DATA_BASE}.ads_activity_stats\n\
    select dt, activity_id, activity_name, start_date, reduce_rate\n\
    from ads_activity_stats as activity\n\
    union\n\
    select '{start_date}'                                                             as dt,\n\
           activity_id,\n\
           activity_name,\n\
           start_date,\n\
           cast(activity_reduce_amount_30d / original_amount_30d as decimal(16, 2)) as reduce_rate\n\
    from {DATA_BASE}.dws_trade_activity_order_nd\n\
    where dt = '{start_date}';"
