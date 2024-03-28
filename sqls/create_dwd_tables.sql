use datahouse;

drop table if exists dwd_trade_cart_add_inc;
create table if not exists dwd_trade_cart_add_inc
(
    id               string comment '编号',
    user_id          string comment '用户 ID',
    sku_id           string comment '商品 ID',
    date_id          string comment '时间 ID',
    create_time      string comment '加购时间',
    source_id        string comment '来源类型 ID',
    source_type_code string comment '来源类型编码',
    source_type_name string comment '来源类型名称',
    sku_num          bigint comment '加购物车件数'
) comment '交易域加购物车事务事实表' 
    partitioned by (`dt` string)
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

drop table if exists dwd_trade_order_detail_inc;
create table if not exists dwd_trade_order_detail_inc
(
    id                    string         comment '编号',
    order_id              string         comment '订单 ID',
    user_id               string         comment '用户 ID',
    sku_id                string         comment '商品 ID',
    province_id           string         comment '省份 ID',
    activity_id           string         comment '参与活动 ID',
    activity_rule_id      string         comment '参与活动规则 ID',
    coupon_id             string         comment '使用优惠券 ID',
    date_id               string         comment '下单日期 ID',
    create_time           string         comment '下单时间',
    source_id             string         comment '来源编号',
    source_type_code      string         comment '来源类型编码',
    source_type_name      string         comment '来源类型名称',
    sku_num               bigint         comment '商品数量',
    split_original_amount decimal(16, 2) comment '原始价格',
    split_activity_amount decimal(16, 2) comment '活动优惠分摊',
    split_coupon_amount   decimal(16, 2) comment '优惠券优惠分摊',
    split_total_amount    decimal(16, 2) comment '最终价格分摊'
) comment '交易域下单明细事务事实表' 
    partitioned by (dt string)
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

drop table if exists dwd_trade_cancel_detail_inc;
create table if not exists dwd_trade_cancel_detail_inc
(
    id                    string         comment '编号',
    order_id              string         comment '订单 ID',
    user_id               string         comment '用户 ID',
    sku_id                string         comment '商品 ID',
    province_id           string         comment '省份 ID',
    activity_id           string         comment '参与活动 ID',
    activity_rule_id      string         comment '参与活动规则 ID',
    coupon_id             string         comment '使用优惠券 ID',
    date_id               string         comment '取消订单日期 ID',
    cancel_time           string         comment '取消订单时间',
    source_id             string         comment '来源编号',
    source_type_code      string         comment '来源类型编码',
    source_type_name      string         comment '来源类型名称',
    sku_num               bigint         comment '商品数量',
    split_original_amount decimal(16, 2) comment '原始价格',
    split_activity_amount decimal(16, 2) comment '活动优惠分摊',
    split_coupon_amount   decimal(16, 2) comment '优惠券优惠分摊',
    split_total_amount    decimal(16, 2) comment '最终价格分摊'
) comment '交易域取消订单明细事务事实表' 
    partitioned by (dt string)
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

drop table if exists dwd_trade_pay_detail_suc_inc;
create table if not exists dwd_trade_pay_detail_suc_inc
(
    id                    string         comment '编号',
    order_id              string         comment '订单 ID',
    user_id               string         comment '用户 ID',
    sku_id                string         comment '商品 ID',
    province_id           string         comment '省份 ID',
    activity_id           string         comment '参与活动规则 ID',
    activity_rule_id      string         comment '参与活动规则 ID',
    coupon_id             string         comment '使用优惠券 ID',
    payment_type_code     string         comment '支付类型编码',
    payment_type_name     string         comment '支付类型名称',
    date_id               string         comment '支付日期 ID',
    callback_time         string         comment '支付成功时间',
    source_id             string         comment '来源编号',
    source_type_code      string         comment '来源类型编码',
    source_type_name      string         comment '来源类型名称',
    sku_num               bigint         comment '商品数量',
    split_original_amount decimal(16, 2) comment '应支付原始金额',
    split_activity_amount decimal(16, 2) comment '支付活动优惠分摊',
    split_coupon_amount   decimal(16, 2) comment '支付优惠券优惠分摊',
    split_payment_amount  decimal(16, 2) comment '支付金额'
) comment '交易域成功支付事务事实表' 
    partitioned by (dt string)
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

-- 交易域退单事务事实表
drop table if exists dwd_trade_order_refund_inc;
create table if not exists dwd_trade_order_refund_inc
(
    id                      string         comment '编号',
    user_id                 string         comment '用户 ID',
    order_id                string         comment '订单 ID',
    sku_id                  string         comment '商品 ID',
    province_id             string         comment '地区 ID',
    date_id                 string         comment '日期 ID',
    create_time             string         comment '退单时间',
    refund_type_code        string         comment '退单类型编码',
    refund_type_name        string         comment '退单类型名称',
    refund_reason_type_code string         comment '退单原因类型编码',
    refund_reason_type_name string         comment '退单原因类型名称',
    refund_reason_txt       string         comment '退单原因描述',
    refund_num              bigint         comment '退单件数',
    refund_amount           decimal(16, 2) comment '退单金额'
) comment '交易域退单事务事实表' 
    partitioned by (dt string) 
    stored as orc
    tblproperties ("orc.compress" = "snappy");

-- 交易域退款成功事务事实表
drop table if exists dwd_trade_refund_pay_suc_inc;
create table if not exists dwd_trade_refund_pay_suc_inc
(
    id                string         comment '编号',
    user_id           string         comment '用户 ID',
    order_id          string         comment '订单编号',
    sku_id            string         comment 'SKU 编号',
    province_id       string         comment '地区 ID',
    payment_type_code string         comment '支付类型编码',
    payment_type_name string         comment '支付类型名称',
    date_id           string         comment '日期 ID',
    callback_time     string         comment '支付成功时间',
    refund_num        decimal(16, 2) comment '退款件数',
    refund_amount     decimal(16, 2) comment '退款金额'
) comment '交易域提交退款成功事务事实表' 
    partitioned by (dt string) 
    stored as orc
    tblproperties ("orc.compress" = "snappy");

-- 交易域购物车周期快照事实表
drop table if exists dwd_trade_cart_full;
create table if not exists dwd_trade_cart_full
(
    id       string comment '编号',
    user_id  string comment '用户 ID',
    sku_id   string comment '商品 ID',
    sku_name string comment '商品名称',
    sku_num  bigint comment '购物车件数'
) comment '交易域购物车周期快照事实表' 
    partitioned by (dt string)
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

-- 工具域优惠券领取事务事实表
drop table if exists dwd_tool_coupon_get_inc;
create table if not exists dwd_tool_coupon_get_inc
(
    id        string comment '编号',
    coupon_id string comment '优惠券 ID',
    user_id   string comment 'USER_ ID',
    date_id   string comment '日期  ID',
    get_time  string comment '领取时间'
) comment '优惠券领取事务事实表' 
    partitioned by (dt string) 
    stored as orc
    tblproperties ("orc.compress" = "snappy");

-- 工具域优惠券使用（下单）：事务事实表
drop table if exists dwd_tool_coupon_order_inc;
create table if not exists dwd_tool_coupon_order_inc
(
    id         string comment '编号',
    coupon_id  string comment '优惠券 ID',
    user_id    string comment '用户 ID',
    order_id   string comment '订单 ID',
    date_id    string comment '日期 ID',
    order_time string comment '使用下单时间'
) comment '优惠券使用下单事务事实表' 
    partitioned by (dt string) 
    stored as orc
    tblproperties ("orc.compress" = "snappy");

-- 工具域优惠券使用（支付）：事务事实表
drop table if exists dwd_tool_coupon_pay_inc;
create table if not exists dwd_tool_coupon_pay_inc
(
    id           string comment '编号',
    coupon_id    string comment '优惠券 ID',
    user_id      string comment '用户 ID',
    order_id     string comment '订单 ID',
    date_id      string comment '日期 ID',
    payment_time string comment '使用支付时间'
) comment '优惠券使用支付事务事实表' 
    partitioned by (dt string) 
    stored as orc
    tblproperties ("orc.compress" = "snappy");

-- 互动域收藏商品事务事实表
drop table if exists dwd_interaction_favor_add_inc;
create table if not exists dwd_interaction_favor_add_inc
(
    id          string comment '编号',
    user_id     string comment '用户 ID',
    sku_id      string comment 'sku_ ID',
    date_id     string comment '日期 ID',
    create_time string comment '收藏时间'
) comment '收藏事实表' 
    partitioned by (dt string) 
    stored as orc
    tblproperties ("orc.compress" = "snappy");

-- 互动域评价事务事实表
drop table if exists dwd_interaction_comment_inc;
create table if not exists dwd_interaction_comment_inc
(
    id            string comment '编号',
    user_id       string comment '用户 ID',
    sku_id        string comment 'SKU ID',
    order_id      string comment '订单 ID',
    date_id       string comment '日期 ID',
    create_time   string comment '评价时间',
    appraise_code string comment '评价编码',
    appraise_name string comment '评价名称'
) comment '评价事务事实表' 
    partitioned by (dt string) 
    stored as orc
    tblproperties ("orc.compress" = "snappy");

-- 流量域页面浏览事务事实表
drop table if exists dwd_traffic_page_view_inc;
create table if not exists dwd_traffic_page_view_inc
(
    province_id    string comment '省份 ID',
    brand          string comment '手机品牌',
    channel        string comment '渠道',
    is_new         string comment '是否首次启动',
    model          string comment '手机型号',
    mid_id         string comment '设备 ID',
    operate_system string comment '操作系统',
    user_id        string comment '会员 ID',
    version_code   string comment 'APP 版本号',
    page_item      string comment '目标 ID',
    page_item_type string comment '目标类型',
    last_page_id   string comment '上页类型',
    page_id        string comment '页面 ID',
    source_type    string comment '来源类型',
    date_id        string comment '日期 ID',
    view_time      string comment '跳入时间',
    session_id     string comment '所属会话 ID',
    during_time    bigint comment '持续时间毫秒'
) comment '页面日志表' 
    partitioned by (dt string) 
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

-- 流量域启动事务事实表
drop table if exists dwd_traffic_start_inc;
create table if not exists dwd_traffic_start_inc
(
    province_id     string comment '省份 ID',
    brand           string comment '手机品牌',
    channel         string comment '渠道',
    is_new          string comment '是否首次启动',
    model           string comment '手机型号',
    mid_id          string comment '设备 ID',
    operate_system  string comment '操作系统',
    user_id         string comment '会员 ID',
    version_code    string comment 'app版本号',
    entry           string comment 'icon手机图标 notice 通知',
    open_ad_id      string comment '广告页ID ',
    date_id         string comment '日期 ID',
    start_time      string comment '启动时间',
    loading_time_ms bigint comment '启动加载时间',
    open_ad_ms      bigint comment '广告总共播放时间',
    open_ad_skip_ms bigint comment '用户跳过广告时点'
) comment '启动日志表' 
    partitioned by (dt string) 
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

-- 流量域动作事务事实表
drop table if exists dwd_traffic_action_inc;
create table if not exists dwd_traffic_action_inc
(
    province_id      string comment '省份 ID',
    brand            string comment '手机品牌',
    channel          string comment '渠道',
    is_new           string comment '是否首次启动',
    model            string comment '手机型号',
    mid_id           string comment '设备 ID',
    operate_system   string comment '操作系统',
    user_id          string comment '会员 ID',
    version_code     string comment 'app版本号',
    during_time      bigint comment '持续时间毫秒',
    page_item        string comment '目标 ID',
    page_item_type   string comment '目标类型',
    last_page_id     string comment '上页类型',
    page_id          string comment '页面 ID',
    source_type      string comment '来源类型',
    action_id        string comment '动作 ID',
    action_item      string comment '目标 ID',
    action_item_type string comment '目标类型',
    date_id          string comment '日期 ID',
    action_time      string comment '动作发生时间'
) comment '动作日志表' 
    partitioned by (dt string) 
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

-- 流量域曝光事务事实表
drop table if exists dwd_traffic_display_inc;
create table if not exists dwd_traffic_display_inc
(
    province_id       string comment '省份 ID',
    brand             string comment '手机品牌',
    channel           string comment '渠道',
    is_new            string comment '是否首次启动',
    model             string comment '手机型号',
    mid_id            string comment '设备 ID',
    operate_system    string comment '操作系统',
    user_id           string comment '会员 ID',
    version_code      string comment 'app版本号',
    during_time       bigint comment 'app版本号',
    page_item         string comment '目标id ',
    page_item_type    string comment '目标类型',
    last_page_id      string comment '上页类型',
    page_id           string comment '页面ID ',
    source_type       string comment '来源类型',
    date_id           string comment '日期 ID',
    display_time      string comment '曝光时间',
    display_type      string comment '曝光类型',
    display_item      string comment '曝光对象id ',
    display_item_type string comment '曝光对象类型',
    display_order     bigint comment '曝光顺序',
    display_pos_id    bigint comment '曝光位置'
) comment '曝光日志表' 
    partitioned by (dt string) 
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

-- 流量域错误事务事实表
drop table if exists dwd_traffic_error_inc;
create table if not exists dwd_traffic_error_inc
(
    province_id     string                                                                           comment '地区编码',
    brand           string                                                                           comment '手机品牌',
    channel         string                                                                           comment '渠道',
    is_new          string                                                                           comment '是否首次启动',
    model           string                                                                           comment '手机型号',
    mid_id          string                                                                           comment '设备 ID',
    operate_system  string                                                                           comment '操作系统',
    user_id         string                                                                           comment '会员 ID',
    version_code    string                                                                           comment 'APP 版本号',
    page_item       string                                                                           comment '目标 ID',
    page_item_type  string                                                                           comment '目标类型',
    last_page_id    string                                                                           comment '上页类型',
    page_id         string                                                                           comment '页面 ID',
    source_type     string                                                                           comment '来源类型',
    entry           string                                                                           comment 'icon 手机图标  notice 通知',
    loading_time    string                                                                           comment '启动加载时间',
    open_ad_id      string                                                                           comment '广告页 ID',
    open_ad_ms      string                                                                           comment '广告总共播放时间',
    open_ad_skip_ms string                                                                           comment '用户跳过广告时点',
    actions         array<struct<action_id: string,    item: string, item_type: string, ts: bigint>> comment '动作信息',
    displays        array<struct<display_type: string, item: string, item_type: string,
                                 `order`: string,       pos_id: string>>                              comment '曝光信息',
    date_id         string                                                                           comment '日期  ID',
    error_time      string                                                                           comment '错误时间',
    error_code      string                                                                           comment '错误码',
    error_msg       string                                                                           comment '错误信息'
) comment '错误日志表' 
    partitioned by (dt string) 
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

-- 用户域用户注册事务事实表
drop table if exists dwd_user_register_inc;
create table if not exists dwd_user_register_inc
(
    user_id        string comment '用户 ID',
    date_id        string comment '日期 ID',
    create_time    string comment '注册时间',
    channel        string comment '应用下载渠道',
    province_id    string comment '省份 ID',
    version_code   string comment '应用版本',
    mid_id         string comment '设备 ID',
    brand          string comment '设备品牌',
    model          string comment '设备型号',
    operate_system string comment '设备操作系统'
) comment '用户域用户注册事务事实表' 
    partitioned by (dt string) 
    stored as orc
    tblproperties ("orc.compress" = "snappy");

-- 用户域用户登录事务事实表
drop table if exists dwd_user_login_inc;
create table if not exists dwd_user_login_inc
(
    user_id        string comment '用户 ID',
    date_id        string comment '日期 ID',
    login_time     string comment '登录时间',
    channel        string comment '应用下载渠道',
    province_id    string comment '省份 ID',
    version_code   string comment '应用版本',
    mid_id         string comment '设备 ID',
    brand          string comment '设备品牌',
    model          string comment '设备型号',
    operate_system string comment '设备操作系统'
) comment '用户域用户登录事务事实表' 
    partitioned by (dt string) 
    stored as orc
    tblproperties ("orc.compress" = "snappy");
