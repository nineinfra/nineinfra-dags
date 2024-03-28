use datahouse;

-- -------------------------------------------------------------------------------------------------
-- ADS 层建表语句
-- -------------------------------------------------------------------------------------------------
-- 各渠道流量统计
drop table if exists ads_traffic_stats_by_channel;
create table if not exists ads_traffic_stats_by_channel
(
    dt               string         comment '统计日期',
    recent_days      bigint         comment '最近天数：1、最近 1 天，7、最近 7 天，30、最近 30 天',
    channel          string         comment '渠道',
    uv_count         bigint         comment '访客人数',
    avg_duration_sec bigint         comment '会话平均停留时长，单位为秒',
    avg_page_count   bigint         comment '会话平均浏览页面数',
    sv_count         bigint         comment '会话数',
    bounce_rate      decimal(16, 2) comment '跳出率'
) comment '各渠道流量统计'
    row format delimited fields terminated by '\t';

-- 路径分析(页面单跳)
drop table if exists ads_page_path;
create table if not exists ads_page_path
(
    dt          string comment '统计日期',
    recent_days bigint comment '最近天数：1、最近 1 天，7、最近 7 天，30、最近 30 天',
    source      string comment '跳转起始页面ID',
    target      string comment '跳转终到页面ID',
    path_count  bigint comment '跳转次数'
) comment '页面浏览路径分析'
    row format delimited fields terminated by '\t';

-- 用户变动统计
drop table if exists ads_user_change;
create table if not exists ads_user_change
(
    dt string               comment '统计日期',
    user_churn_count bigint comment '流失用户数(新增)',
    user_back_count  bigint comment '回流用户数'
) comment '用户变动统计'
    row format delimited fields terminated by '\t';

-- 用户留存率
drop table if exists ads_user_retention;
create table if not exists ads_user_retention
(
    dt              string         comment '统计日期',
    create_date     string         comment '用户新增日期',
    retention_day   int            comment '截至当前日期留存天数',
    retention_count bigint         comment '留存用户数量',
    new_user_count  bigint         comment '新增用户数量',
    retention_rate  decimal(16, 2) comment '留存率'
) comment '用户留存率'
    row format delimited fields terminated by '\t';

-- 用户新增活跃统计
drop table if exists ads_user_stats;
create table if not exists ads_user_stats
(
    dt                string comment '统计日期',
    recent_days       bigint comment '最近 N 日：1、最近 1 日，7、最近 7 日，30、最近 30 日',
    new_user_count    bigint comment '新增用户数',
    active_user_count bigint comment '活跃用户数'
) comment '用户新增活跃统计'
    row format delimited fields terminated by '\t';

-- 用户行为漏斗分析
drop table if exists ads_user_action;
create table if not exists ads_user_action
(
    dt                string comment '统计日期',
    recent_days       bigint comment '最近天数：1、最近 1 天，7、最近 7 天，30、最近 30 天',
    home_count        bigint comment '浏览首页人数',
    good_detail_count bigint comment '浏览商品详情页人数',
    cart_count        bigint comment '加入购物车人数',
    order_count       bigint comment '下单人数',
    payment_count     bigint comment '支付人数'
) comment '漏斗分析'
    row format delimited fields terminated by '\t';

-- 新增交易用户统计
drop table if exists ads_new_buyer_stats;
create table if not exists ads_new_buyer_stats
(
    dt                     string comment '统计日期',
    recent_days            bigint comment '最近天数：1、最近 1 天，7、最近 7 天，30、最近 30 天',
    new_order_user_count   bigint comment '新增下单人数',
    new_payment_user_count bigint comment '新增支付人数'
) comment '新增交易用户统计'
    row format delimited fields terminated by '\t';

-- 最近 7/30 日各品牌复购率
drop table if exists ads_repeat_purchase_by_tm;
create table if not exists ads_repeat_purchase_by_tm
(
    dt                string comment '统计日期',
    recent_days       bigint comment '最近天数：7、最近 7 天，30、最近 30 天',
    tm_id             string comment '品牌 ID',
    tm_name           string comment '品牌名称',
    order_repeat_rate decimal(16, 2) comment '复购率'
) comment '各品牌复购率统计'
    row format delimited fields terminated by '\t';

-- 各品牌商品交易统计
drop table if exists ads_trade_stats_by_tm;
create table if not exists ads_trade_stats_by_tm
(
    dt                      string comment '统计日期',
    recent_days             bigint comment '最近天数：1、最近 1 天，7、最近 7 天，30、最近 30 天',
    tm_id                   string comment '品牌ID',
    tm_name                 string comment '品牌名称',
    order_count             bigint comment '订单数',
    order_user_count        bigint comment '订单人数',
    order_refund_count      bigint comment '退单数',
    order_refund_user_count bigint comment '退单人数'
) comment '各品牌商品交易统计'
    row format delimited fields terminated by '\t';

-- 各品类商品交易统计
drop table if exists ads_trade_stats_by_cate;
create table if not exists ads_trade_stats_by_cate
(
    dt                      string comment '统计日期',
    recent_days             bigint comment '最近天数：1、最近 1 天，7、最近 7 天，30、最近 30 天',
    category1_id            string comment '一级分类id',
    category1_name          string comment '一级分类名称',
    category2_id            string comment '二级分类id',
    category2_name          string comment '二级分类名称',
    category3_id            string comment '三级分类id',
    category3_name          string comment '三级分类名称',
    order_count             bigint comment '订单数',
    order_user_count        bigint comment '订单人数',
    order_refund_count      bigint comment '退单数',
    order_refund_user_count bigint comment '退单人数'
) comment '各分类商品交易统计'
    row format delimited fields terminated by '\t';

-- 各分类商品购物车存量 Top3
drop table if exists ads_sku_cart_num_top3_by_cate;
create table if not exists ads_sku_cart_num_top3_by_cate
(
    dt             string comment '统计日期',
    category1_id   string comment '一级分类 ID',
    category1_name string comment '一级分类名称',
    category2_id   string comment '二级分类 ID',
    category2_name string comment '二级分类名称',
    category3_id   string comment '三级分类 ID',
    category3_name string comment '三级分类名称',
    sku_id         string comment '商品 ID',
    sku_name       string comment '商品名称',
    cart_num       bigint comment '购物车中商品数量',
    rk             bigint comment '排名'
) comment '各分类商品购物车存量 Top10'
    row format delimited fields terminated by '\t';

-- 交易主题：交易综合统计
drop table if exists ads_trade_stats;
create table if not exists ads_trade_stats
(
    dt                      string         comment '统计日期',
    recent_days             bigint         comment '最近天数,1:最近1日,7:最近7天,30:最近30天',
    order_total_amount      decimal(16, 2) comment '订单总额,GMV',
    order_count             bigint         comment '订单数',
    order_user_count        bigint         comment '下单人数',
    order_refund_count      bigint         comment '退单数',
    order_refund_user_count bigint         comment '退单人数'
) comment '交易统计'
    row format delimited fields terminated by '\t';

-- 各省份交易统计
drop table if exists ads_order_by_province;
create table if not exists ads_order_by_province
(
    dt                 string         comment '统计日期',
    recent_days        bigint         comment '最近天数：1、最近 1 天，7、最近 7 天，30、最近 30 天',
    province_id        string         comment '省份ID',
    province_name      string         comment '省份名称',
    area_code          string         comment '地区编码',
    iso_code           string         comment '国际标准地区编码',
    iso_code_3166_2    string         comment '国际标准地区编码',
    order_count        bigint         comment '订单数',
    order_total_amount decimal(16, 2) comment '订单金额'
) comment '各地区订单统计'
    row format delimited fields terminated by '\t';

-- 优惠券主题：最近 30天发布的优惠券的补贴率
drop table if exists ads_coupon_stats;
create table if not exists ads_coupon_stats
(
    dt          string         comment '统计日期',
    coupon_id   string         comment '优惠券ID',
    coupon_name string         comment '优惠券名称',
    start_date  string         comment '发布日期',
    rule_name   string         comment '优惠规则，例如满 100 元减 10 元',
    reduce_rate decimal(16, 2) comment '补贴率'
) comment '优惠券统计'
    row format delimited fields terminated by '\t';

-- 活动主题：最近 30 天发布的活动的补贴率
drop table if exists ads_activity_stats;
create table if not exists ads_activity_stats
(
    dt            string         comment '统计日期',
    activity_id   string         comment '活动 ID',
    activity_name string         comment '活动名称',
    start_date    string         comment '活动开始日期',
    reduce_rate   decimal(16, 2) comment '补贴率'
) comment '活动统计'
    row format delimited fields terminated by '\t';
