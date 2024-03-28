use datahouse;

-- -------------------------------------------------------------------------------------------------
-- DWS 层建表语句
-- -------------------------------------------------------------------------------------------------
-- 交易域用户商品粒度订单最近 1 日汇总表
drop table if exists dws_trade_user_sku_order_1d;
create table if not exists dws_trade_user_sku_order_1d
(
    user_id                   string         comment '用户 ID',
    sku_id                    string         comment 'SKU ID',
    sku_name                  string         comment 'SKU 名称',
    category1_id              string         comment '一级分类 ID',
    category1_name            string         comment '一级分类名称',
    category2_id              string         comment '一级分类 ID',
    category2_name            string         comment '一级分类名称',
    category3_id              string         comment '一级分类 ID',
    category3_name            string         comment '一级分类名称',
    tm_id                     string         comment '品牌 ID',
    tm_name                   string         comment '品牌名称',
    order_count_1d            bigint         comment '最近 1 日下单次数',
    order_num_1d              bigint         comment '最近 1 日下单件数',
    order_original_amount_1d  decimal(16, 2) comment '最近 1 日下单原始金额',
    activity_reduce_amount_1d decimal(16, 2) comment '最近 1 日活动优惠金额',
    coupon_reduce_amount_1d   decimal(16, 2) comment '最近 1 日优惠券优惠金额',
    order_total_amount_1d     decimal(16, 2) comment '最近 1 日下单最终金额'
) comment '交易域用户商品粒度订单最近 1 日汇总事实表'
    partitioned by (dt string)
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

-- 交易域用户商品粒度退单最近 1 日汇总表
drop table if exists dws_trade_user_sku_order_refund_1d;
create table if not exists dws_trade_user_sku_order_refund_1d
(
    user_id                string         comment '用户 ID',
    sku_id                 string         comment 'SKU ID',
    sku_name               string         comment 'SKU 名称',
    category1_id           string         comment '一级分类 ID',
    category1_name         string         comment '一级分类名称',
    category2_id           string         comment '一级分类 ID',
    category2_name         string         comment '一级分类名称',
    category3_id           string         comment '一级分类 ID',
    category3_name         string         comment '一级分类名称',
    tm_id                  string         comment '品牌 ID',
    tm_name                string         comment '品牌名称',
    order_refund_count_1d  bigint         comment '最近 1 日退单次数',
    order_refund_num_1d    bigint         comment '最近 1 日退单件数',
    order_refund_amount_1d decimal(16, 2) comment '最近 1 日退单金额'
) comment '交易域用户商品粒度退单最近 1 日汇总事实表'
    partitioned by (dt string)
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

-- 交易域用户粒度订单最近 1 日汇总表
drop table if exists dws_trade_user_order_1d;
create table if not exists dws_trade_user_order_1d
(
    user_id                   string         comment '用户 ID',
    order_count_1d            bigint         comment '最近 1 日下单次数',
    order_num_1d              bigint         comment '最近 1 日下单商品件数',
    order_original_amount_1d  decimal(16, 2) comment '最近 1 日最近 1 日下单原始金额',
    activity_reduce_amount_1d decimal(16, 2) comment '最近 1 日下单活动优惠金额',
    coupon_reduce_amount_1d   decimal(16, 2) comment '下单优惠券优惠金额',
    order_total_amount_1d     decimal(16, 2) comment '最近 1 日下单最终金额'
) comment '交易域用户粒度订单最近 1 日汇总事实表'
    partitioned by (dt string)
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

-- 交易域用户粒度加购最近 1 日汇总表
drop table if exists dws_trade_user_cart_add_1d;
create table if not exists dws_trade_user_cart_add_1d
(
    user_id           string comment '用户 ID',
    cart_add_count_1d bigint comment '最近 1 日加购次数',
    cart_add_num_1d   bigint comment '最近 1 日加购商品件数'
) comment '交易域用户粒度加购最近 1 日汇总事实表'
    partitioned by (dt string)
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

-- 交易域用户粒度支付最近 1 日汇总表
drop table if exists dws_trade_user_payment_1d;
create table if not exists dws_trade_user_payment_1d
(
    user_id           string         comment '用户 ID',
    payment_count_1d  bigint         comment '最近 1 日支付次数',
    payment_num_1d    bigint         comment '最近 1 日支付商品件数',
    payment_amount_1d decimal(16, 2) comment '最近 1 日支付金额'
) comment '交易域用户粒度支付最近 1 日汇总事实表'
    partitioned by (dt string)
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

-- 最近 1 交易域用户粒度退单最近 1 日汇总表
drop table if exists dws_trade_user_order_refund_1d;
create table if not exists dws_trade_user_order_refund_1d
(
    user_id                string         comment '用户 ID',
    order_refund_count_1d  bigint         comment '最近 1 日退单次数',
    order_refund_num_1d    bigint         comment '最近 1 日退单商品件数',
    order_refund_amount_1d decimal(16, 2) comment '最近 1 日退单金额'
) comment '交易域用户粒度退单最近 1 日汇总事实表'
    partitioned by (dt string)
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

-- 交易域省份粒度订单最近 1 日汇总表
drop table if exists dws_trade_province_order_1d;
create table if not exists dws_trade_province_order_1d
(
    province_id               string         comment '省份 ID',
    province_name             string         comment '省份名称',
    area_code                 string         comment '地区编码',
    iso_code                  string         comment '旧版 ISO-3166-2 编码',
    iso_3166_2                string         comment '新版版 ISO-3166-2 编码',
    order_count_1d            bigint         comment '最近 1 日下单次数',
    order_original_amount_1d  decimal(16, 2) comment '最近 1 日下单原始金额',
    activity_reduce_amount_1d decimal(16, 2) comment '最近 1 日下单活动优惠金额',
    coupon_reduce_amount_1d   decimal(16, 2) comment '最近 1 日下单优惠券优惠金额',
    order_total_amount_1d     decimal(16, 2) comment '最近 1 日下单最终金额'
) comment '交易域省份粒度订单最近 1 日汇总事实表'
    partitioned by (dt string)
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

-- 流量域会话粒度页面浏览最近 1 日汇总表
drop table if exists dws_traffic_session_page_view_1d;
create table if not exists dws_traffic_session_page_view_1d
(
    session_id     string comment '会话 ID',
    mid_id         string comment '设备 ID',
    brand          string comment '手机品牌',
    model          string comment '手机型号',
    operate_system string comment '操作系统',
    version_code   string comment 'APP 版本号',
    channel        string comment '渠道',
    during_time_1d bigint comment '最近 1 日访问时长',
    page_count_1d  bigint comment '最近 1 日访问页面数'
) comment '流量域会话粒度页面浏览最近 1 日汇总表'
    partitioned by (dt string)
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

-- 流量域访客页面粒度页面浏览最近 1 日汇总表
drop table if exists dws_traffic_page_visitor_page_view_1d;
create table if not exists dws_traffic_page_visitor_page_view_1d
(
    mid_id         string comment '访客 ID',
    brand          string comment '手机品牌',
    model          string comment '手机型号',
    operate_system string comment '操作系统',
    page_id        string comment '页面 ID',
    during_time_1d bigint comment '最近 1 日浏览时长',
    view_count_1d  bigint comment '最近 1 日访问次数'
) comment '流量域访客页面粒度页面浏览最近 1 日汇总事实表'
    partitioned by (dt string)
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

-- 最近 N 日汇总表：交易域用户商品粒度订单最近 N 日汇总表
drop table if exists dws_trade_user_sku_order_nd;
create table if not exists dws_trade_user_sku_order_nd
(
    user_id                    string         comment '用户 ID',
    sku_id                     string         comment 'SKU ID',
    sku_name                   string         comment 'SKU 名称',
    category1_id               string         comment '一级分类 ID',
    category1_name             string         comment '一级分类名称',
    category2_id               string         comment '一级分类 ID',
    category2_name             string         comment '一级分类名称',
    category3_id               string         comment '一级分类 ID',
    category3_name             string         comment '一级分类名称',
    tm_id                      string         comment '品牌 ID',
    tm_name                    string         comment '品牌名称',
    order_count_7d             string         comment '最近 7 日下单次数',
    order_num_7d               bigint         comment '最近 7 日下单件数',
    order_original_amount_7d   decimal(16, 2) comment '最近 7 日下单原始金额',
    activity_reduce_amount_7d  decimal(16, 2) comment '最近 7 日活动优惠金额',
    coupon_reduce_amount_7d    decimal(16, 2) comment '最近 7 日优惠券优惠金额',
    order_total_amount_7d      decimal(16, 2) comment '最近 7 日下单最终金额',
    order_count_30d            bigint         comment '最近 30 日下单次数',
    order_num_30d              bigint         comment '最近 30 日下单件数',
    order_original_amount_30d  decimal(16, 2) comment '最近 30 日下单原始金额',
    activity_reduce_amount_30d decimal(16, 2) comment '最近 30 日活动优惠金额',
    coupon_reduce_amount_30d   decimal(16, 2) comment '最近 30 日优惠券优惠金额',
    order_total_amount_30d     decimal(16, 2) comment '最近 30 日下单最终金额'
) comment '交易域用户商品粒度订单最近 N 日汇总事实表'
    partitioned by (dt string)
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

-- 交易域用户商品粒度退单最近 N 日汇总表
drop table if exists dws_trade_user_sku_order_refund_nd;
create table if not exists dws_trade_user_sku_order_refund_nd
(
    user_id                 string         comment '用户 ID',
    sku_id                  string         comment 'SKU ID',
    sku_name                string         comment 'SKU 名称',
    category1_id            string         comment '一级分类 ID',
    category1_name          string         comment '一级分类名称',
    category2_id            string         comment '一级分类 ID',
    category2_name          string         comment '一级分类名称',
    category3_id            string         comment '一级分类 ID',
    category3_name          string         comment '一级分类名称',
    tm_id                   string         comment '品牌 ID',
    tm_name                 string         comment '品牌名称',
    order_refund_count_7d   bigint         comment '最近 7 日退单次数',
    order_refund_num_7d     bigint         comment '最近 7 日退单件数',
    order_refund_amount_7d  decimal(16, 2) comment '最近 7 日退单金额',
    order_refund_count_30d  bigint         comment '最近 30 日退单次数',
    order_refund_num_30d    bigint         comment '最近 30 日退单件数',
    order_refund_amount_30d decimal(16, 2) comment '最近 30 日退单金额'
) comment '交易域用户商品粒度退单最近 N 日汇总事实表'
    partitioned by (dt string)
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

-- 交易域用户粒度订单最近 N 日汇总表
drop table if exists dws_trade_user_order_nd;
create table if not exists dws_trade_user_order_nd
(
    user_id                    string         comment '用户 ID',
    order_count_7d             bigint         comment '最近 7 日下单次数',
    order_num_7d               bigint         comment '最近 7 日下单商品件数',
    order_original_amount_7d   decimal(16, 2) comment '最近 7 日下单原始金额',
    activity_reduce_amount_7d  decimal(16, 2) comment '最近 7 日下单活动优惠金额',
    coupon_reduce_amount_7d    decimal(16, 2) comment '最近 7 日下单优惠券优惠金额',
    order_total_amount_7d      decimal(16, 2) comment '最近 7 日下单最终金额',
    order_count_30d            bigint         comment '最近 30 日下单次数',
    order_num_30d              bigint         comment '最近 30 日下单商品件数',
    order_original_amount_30d  decimal(16, 2) comment '最近 30 日下单原始金额',
    activity_reduce_amount_30d decimal(16, 2) comment '最近 30 日下单活动优惠金额',
    coupon_reduce_amount_30d   decimal(16, 2) comment '最近 30 日下单优惠券优惠金额',
    order_total_amount_30d     decimal(16, 2) comment '最近 30 日下单最终金额'
) comment '交易域用户粒度订单最近 N 日汇总事实表'
    partitioned by (dt string)
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

-- 交易域用户粒度加购最近 N 日汇总表
drop table if exists dws_trade_user_cart_add_nd;
create table if not exists dws_trade_user_cart_add_nd
(
    user_id            string comment '用户 ID',
    cart_add_count_7d  bigint comment '最近 7 日加购次数',
    cart_add_num_7d    bigint comment '最近 7 日加购商品件数',
    cart_add_count_30d bigint comment '最近 30 日加购次数',
    cart_add_num_30d   bigint comment '最近 30 日加购商品件数'
) comment '交易域用户粒度加购最近 N 日汇总事实表'
    partitioned by (dt string)
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

-- 交易域用户粒度支付最近 N 日汇总表
drop table if exists dws_trade_user_payment_nd;
create table if not exists dws_trade_user_payment_nd
(
    user_id            string         comment '用户 ID',
    payment_count_7d   bigint         comment '最近 7 日支付次数',
    payment_num_7d     bigint         comment '最近 7 日支付商品件数',
    payment_amount_7d  decimal(16, 2) comment '最近 7 日支付金额',
    payment_count_30d  bigint         comment '最近 30 日支付次数',
    payment_num_30d    bigint         comment '最近 30 日支付商品件数',
    payment_amount_30d decimal(16, 2) comment '最近 30 日支付金额'
) comment '交易域用户粒度支付最近 N 日汇总事实表'
    partitioned by (dt string)
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

-- 交易域用户粒度退单最近 N 日汇总表
drop table if exists dws_trade_user_order_refund_nd;
create table if not exists dws_trade_user_order_refund_nd
(
    user_id                 string         comment '用户 ID',
    order_refund_count_7d   bigint         comment '最近 7 日退单次数',
    order_refund_num_7d     bigint         comment '最近 7 日退单商品件数',
    order_refund_amount_7d  decimal(16, 2) comment '最近 7 日退单金额',
    order_refund_count_30d  bigint         comment '最近 30 日退单次数',
    order_refund_num_30d    bigint         comment '最近 30 日退单商品件数',
    order_refund_amount_30d decimal(16, 2) comment '最近 30 日退单金额'
) comment '交易域用户粒度退单最近 N 日汇总事实表'
    partitioned by (dt string)
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

-- 交易域省份粒度订单最近 N 日汇总表
drop table if exists dws_trade_province_order_nd;
create table if not exists dws_trade_province_order_nd
(
    province_id                string         comment '省份 ID',
    province_name              string         comment '省份名称',
    area_code                  string         comment '地区编码',
    iso_code                   string         comment '旧版 ISO-3166-2 编码',
    iso_3166_2                 string         comment '新版 ISO-3166-2 编码',
    order_count_7d             bigint         comment '最近 7 日下单次数',
    order_original_amount_7d   decimal(16, 2) comment '最近 7 日下单原始金额',
    activity_reduce_amount_7d  decimal(16, 2) comment '最近 7 日下单活动优惠金额',
    coupon_reduce_amount_7d    decimal(16, 2) comment '最近 7 日下单优惠券优惠金额',
    order_total_amount_7d      decimal(16, 2) comment '最近 7 日下单最终金额',
    order_count_30d            bigint         comment '最近 30 日下单次数',
    order_original_amount_30d  decimal(16, 2) comment '最近 30 日下单原始金额',
    activity_reduce_amount_30d decimal(16, 2) comment '最近 30 日下单活动优惠金额',
    coupon_reduce_amount_30d   decimal(16, 2) comment '最近 30 日下单优惠券优惠金额',
    order_total_amount_30d     decimal(16, 2) comment '最近 30 日下单最终金额'
) comment '交易域省份粒度订单最近 N 日汇总事实表'
    partitioned by (dt string)
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

-- 交易域优惠券粒度订单最近 N 日汇总表
drop table if exists dws_trade_coupon_order_nd;
create table if not exists dws_trade_coupon_order_nd
(
    coupon_id                string         comment '优惠券 ID',
    coupon_name              string         comment '优惠券名称',
    coupon_type_code         string         comment '优惠券类型 ID',
    coupon_type_name         string         comment '优惠券类型名称',
    coupon_rule              string         comment '优惠券规则',
    start_date               string         comment '发布日期',
    original_amount_30d      decimal(16, 2) comment '使用下单原始金额',
    coupon_reduce_amount_30d decimal(16, 2) comment '使用下单优惠金额'
) comment '交易域优惠券粒度订单最近 N 日汇总事实表'
    partitioned by (dt string)
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

-- 交易域活动粒度订单最近 N 日汇总表
drop table if exists dws_trade_activity_order_nd;
create table if not exists dws_trade_activity_order_nd
(
    activity_id                string         comment '活动 ID',
    activity_name              string         comment '活动名称',
    activity_type_code         string         comment '活动类型编码',
    activity_type_name         string         comment '活动类型名称',
    start_date                 string         comment '发布日期',
    original_amount_30d        decimal(16, 2) comment '参与活动订单原始金额',
    activity_reduce_amount_30d decimal(16, 2) comment '参与活动订单优惠金额'
) comment '交易域活动粒度订单最近 N 日汇总事实表'
    partitioned by (dt string)
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

-- 流量域访客页面粒度页面浏览最近 N 日汇总表
drop table if exists dws_traffic_page_visitor_page_view_nd;
create table if not exists dws_traffic_page_visitor_page_view_nd
(
    mid_id          string comment '访客 ID',
    brand           string comment '手机品牌',
    model           string comment '手机型号',
    operate_system  string comment '操作系统',
    page_id         string comment '页面 ID',
    during_time_7d  bigint comment '最近 7 日浏览时长',
    view_count_7d   bigint comment '最近 7 日访问次数',
    during_time_30d bigint comment '最近 30 日浏览时长',
    view_count_30d  bigint comment '最近 30 日访问次数'
) comment '流量域访客页面粒度页面浏览最近 N 日汇总事实表'
    partitioned by (dt string)
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

-- 历史至今汇总表：交易域用户粒度订单历史至今汇总表
drop table if exists dws_trade_user_order_td;
create table if not exists dws_trade_user_order_td
(
    user_id                   string         comment '用户 ID',
    order_date_first          string         comment '首次下单日期',
    order_date_last           string         comment '末次下单日期',
    order_count_td            bigint         comment '历史至今下单次数',
    order_num_td              bigint         comment '历史至今购买商品件数',
    original_amount_td        decimal(16, 2) comment '历史至今原始金额',
    activity_reduce_amount_td decimal(16, 2) comment '历史至今活动优惠金额',
    coupon_reduce_amount_td   decimal(16, 2) comment '历史至今优惠券优惠金额',
    total_amount_td           decimal(16, 2) comment '历史至今最终金额'
) comment '交易域用户粒度订单历史至今汇总事实表'
    partitioned by (dt string)
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

-- 交易域用户粒度支付历史至今汇总表
drop table if exists dws_trade_user_payment_td;
create table if not exists dws_trade_user_payment_td
(
    user_id            string         comment '用户 ID',
    payment_date_first string         comment '首次支付日期',
    payment_date_last  string         comment '末次支付日期',
    payment_count_td   bigint         comment '历史至今支付次数',
    payment_num_td     bigint         comment '历史至今支付商品件数',
    payment_amount_td  decimal(16, 2) comment '历史至今支付金额'
) comment '交易域用户粒度支付历史至今汇总事实表'
    partitioned by (dt string)
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

-- 用户域用户粒度登录历史至今汇总表
drop table if exists dws_user_user_login_td;
create table if not exists dws_user_user_login_td
(
    user_id         string comment '用户 ID',
    login_date_last string comment '末次登录日期',
    login_count_td  bigint comment '累计登录次数'
) comment '用户域用户粒度登录历史至今汇总事实表'
    partitioned by (dt string)
    stored as orc
    tblproperties ('orc.compress' = 'snappy');