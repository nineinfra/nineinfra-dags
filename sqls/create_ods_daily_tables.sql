use datahouse;

-- -------------------------------------------------------------------------------------------------
-- 购物车表（日增表）
-- -------------------------------------------------------------------------------------------------
drop table if exists ods_cart_info_daily;
create table  if not exists ods_cart_info_daily
(
	id           string         comment '编号',
	user_id      string         comment '用户 ID',
	sku_id       string         comment 'SKU_ID',
	cart_price   decimal(16, 2) comment '放入购物车时价格',
	sku_num      bigint         comment '数量',
	img_url      bigint         comment '商品图片地址',
	sku_name     string         comment 'sku 名称 (冗余)',
	is_checked   string         comment '是否被选中',
	create_time  string         comment '创建时间',
	operate_time string         comment '修改时间',
	is_ordered   string         comment '是否已经下单',
	order_time   string         comment '下单时间',
	source_type  string         comment '来源类型',
	source_id    string         comment '来源编号'
) comment '购物车全量表' 
    partitioned by (dt string) 
    row format delimited fields terminated by '\t' null defined as '';


-- -------------------------------------------------------------------------------------------------
-- 商品评论表（日增表）
-- -------------------------------------------------------------------------------------------------
drop table if exists ods_comment_info_daily;
create table  if not exists ods_comment_info_daily
(
	id           string         comment '编号',
	user_id      string         comment '用户 ID',
    nick_name    string         comment '用户昵称',
    head_img     string         comment '用户头像',
	sku_id       string         comment 'SKU_ID',
	spu_id       string         comment '商品 ID',
	order_id     string         comment '订单编号',
	appraise     string         comment '评价 1 好评 2 中评 3 差评',
	comment_txt  string         comment '评价内容',
	create_time  string         comment '创建时间',
	operate_time string         comment '修改时间',
) comment '商品评论日增表'
    partitioned by (dt string)
    row format delimited fields terminated by '\t' null defined as '';

-- -------------------------------------------------------------------------------------------------
-- 优惠券领用表（日增表）
-- -------------------------------------------------------------------------------------------------
drop table if exists ods_coupon_use_daily;
create table  if not exists ods_coupon_use_daily
(
	id           string         comment '编号',
	coupon_id    string         comment '购物券 ID',
	user_id      string         comment '用户 ID',
	order_id     string         comment '订单 ID',
	coupon_status string        comment '购物券状态（1：未使用 2：已使用）',
	get_time     string         comment '获取时间',
	using_time   string         comment '使用时间',
	used_time    string         comment '支付时间',
	expire_time  string         comment '过期时间',
) comment '优惠券领用日增表'
    partitioned by (dt string)
    row format delimited fields terminated by '\t' null defined as '';


-- -------------------------------------------------------------------------------------------------
-- 商品收藏表（日增表）
-- -------------------------------------------------------------------------------------------------
drop table if exists ods_favor_info_daily;
create table  if not exists ods_favor_info_daily
(
	id           string         comment '编号',
	user_id      string         comment '用户 ID',
	sku_id       string         comment 'SKU_ID',
    spu_id       string         comment '商品 ID',
    is_cancel    string         comment '是否已取消 0 正常 1 已取消',
    create_time  string         comment '创建时间',
    cancel_time  string         comment '修改时间',
) comment '商品收藏日增表'
    partitioned by (dt string)
    row format delimited fields terminated by '\t' null defined as '';


-- -------------------------------------------------------------------------------------------------
-- 订单明细表（日增表）
-- -------------------------------------------------------------------------------------------------
drop table if exists ods_order_detail_daily;
create table  if not exists ods_order_detail_daily
(
	id           string         comment '编号',
	order_id     string         comment '订单编号',
	sku_id       string         comment 'sku_id',
	sku_name     string         comment 'sku名称（冗余)',
	img_url      string         comment '图片名称（冗余)',
	order_price  decimal(10,2)  comment '购买价格(下单时sku价格）',
	sku_num      bigint         comment '购买个数',
	create_time  string         comment '创建时间',
	source_type  string         comment '来源类型',
	source_id    string         comment '来源编号',
	split_total_amount decimal(16,2) comment '总金额',
	split_activity_amount decimal(16,2) comment '活动金额',
	split_coupon_amount decimal(16,2) comment '优惠券金额',
) comment '订单明细日增表'
    partitioned by (dt string)
    row format delimited fields terminated by '\t' null defined as '';

-- -------------------------------------------------------------------------------------------------
-- 订单明细活动表（日增表）
-- -------------------------------------------------------------------------------------------------
drop table if exists ods_order_detail_activity_daily;
create table  if not exists ods_order_detail_activity_daily
(
	id           string         comment '编号',
	order_id     string         comment '订单编号',
	order_detail_id     string         comment '订单明细编号',
	activity_id  string         comment '活动ID',
	activity_rule_id  string         comment '活动规则',
	sku_id       string         comment 'sku_id',
	create_time  string         comment '创建时间',
) comment '订单明细活动日增表'
    partitioned by (dt string)
    row format delimited fields terminated by '\t' null defined as '';

-- -------------------------------------------------------------------------------------------------
-- 订单明细购物券表（日增表）
-- -------------------------------------------------------------------------------------------------
drop table if exists ods_order_detail_coupon_daily;
create table  if not exists ods_order_detail_coupon_daily
(
	id           string         comment '编号',
	order_id     string         comment '订单编号',
	order_detail_id     string         comment '订单明细编号',
	coupon_id    string         comment '购物券ID',
	coupon_use_id string        comment '购物券领用id',
	sku_id       string         comment 'sku_id',
	create_time  string         comment '创建时间',
) comment '订单明细购物券日增表'
    partitioned by (dt string)
    row format delimited fields terminated by '\t' null defined as '';

-- -------------------------------------------------------------------------------------------------
-- 订单表（日增表）
-- -------------------------------------------------------------------------------------------------
drop table if exists ods_order_info_daily;
create table  if not exists ods_order_info_daily
(
	id           string         comment '编号',
	consignee     string        comment '收货人',
	consignee_tel string        comment '收件人电话',
	total_amount  decimal(10,2) comment '总金额',
	order_status  string        comment '订单状态',
	user_id       string        comment '用户id',
	payment_way   string        comment '付款方式',
	delivery_address string      comment '送货地址',
	order_comment string         comment '订单备注',
	out_trade_no  string         comment '订单交易编号（第三方支付用)',
	trade_body    string         comment '订单描述(第三方支付用)',
	create_time   string         comment '创建时间',
	operate_time  string         comment '操作时间',
	expire_time   string         comment '失效时间',
	process_status string        comment '进度状态',
	tracking_no   string         comment '物流单编号',
	parent_order_id string       comment '父订单编号',
	img_url       string         comment '图片路径',
	province_id   string         comment '地区',
	activity_reduce_amount decimal(16,2) comment '促销金额',
	coupon_reduce_amount decimal(16,2) comment '优惠券',
	original_total_amount decimal(16,2) comment '原价金额',
	feight_fee    decimal(16,2) comment '运费',
	feight_fee_reduce decimal(16,2) comment '运费减免',
	refundable_time string        comment '可退款日期（签收后30天）',
) comment '订单日增表'
    partitioned by (dt string)
    row format delimited fields terminated by '\t' null defined as '';

-- -------------------------------------------------------------------------------------------------
-- 退单表（日增表）
-- -------------------------------------------------------------------------------------------------
drop table if exists ods_order_refund_info_daily;
create table  if not exists ods_order_refund_info_daily
(
	id           string         comment '编号',
	user_id      string         comment '用户 ID',
	order_id     string         comment '订单 ID',
	sku_id       string         comment 'SKU_ID',
	refund_type  string         comment '退款类型',
	refund_num   bigint         comment '退货件数',
	refund_amount decimal(16,2) comment '退款金额',
	refund_reason_type string    comment '原因类型',
	refund_reason_txt string      comment '原因内容',
	refund_status string         comment '退款状态（0：待审批 1：已退款）',
	create_time  string         comment '创建时间',
) comment '退单日增表'
    partitioned by (dt string)
    row format delimited fields terminated by '\t' null defined as '';

-- -------------------------------------------------------------------------------------------------
-- 订单状态日志记录表（日增表）
-- -------------------------------------------------------------------------------------------------
drop table if exists ods_order_status_log_daily;
create table  if not exists ods_order_status_log_daily
(
	id           string         comment '编号',
	order_id     string         comment '订单 ID',
	order_status string         comment '订单状态',
	operate_time string         comment '操作时间',
) comment '订单状态日志记录日增表'
    partitioned by (dt string)
    row format delimited fields terminated by '\t' null defined as '';

-- -------------------------------------------------------------------------------------------------
-- 支付信息表（日增表）
-- -------------------------------------------------------------------------------------------------
drop table if exists ods_payment_info_daily;
create table  if not exists ods_payment_info_daily
(
	id           string         comment '编号',
	out_trade_no string         comment '对外业务编号',
	order_id     string         comment '订单编号',
	user_id      string         comment '用户 ID',
	payment_type string         comment '支付类型（微信 支付宝）',
	trade_no     string         comment '交易编号',
	total_amount decimal(10,2)  comment '支付金额',
	subject      string         comment '交易内容',
	payment_status string        comment '支付状态',
	create_time  string         comment '创建时间',
	callback_time string         comment '回调时间',
	callback_content string      comment '回调信息',
) comment '支付信息日增表'
    partitioned by (dt string)
    row format delimited fields terminated by '\t' null defined as '';


-- -------------------------------------------------------------------------------------------------
-- 退款信息表（日增表）
-- -------------------------------------------------------------------------------------------------
drop table if exists ods_refund_payment_daily;
create table  if not exists ods_refund_payment_daily
(
	id           string         comment '编号',
	out_trade_no string         comment '对外业务编号',
	order_id     string         comment '订单编号',
	sku_id       string         comment 'SKU_ID',
	payment_type string         comment '支付类型（微信 支付宝）',
	trade_no     string         comment '交易编号',
	total_amount decimal(10,2)  comment '退款金额',
	subject      string         comment '交易内容',
	refund_status string        comment '退款状态',
	create_time  string         comment '创建时间',
	callback_time string         comment '回调时间',
	callback_content string      comment '回调信息',
) comment '退款信息日增表'
    partitioned by (dt string)
    row format delimited fields terminated by '\t' null defined as '';


-- -------------------------------------------------------------------------------------------------
-- 用户详细信息表（日增表）
-- -------------------------------------------------------------------------------------------------
drop table if exists ods_user_info_daily;
create table  if not exists ods_user_info_daily
(
	id           string         comment '编号',
	login_name   string         comment '用户名称',
	nick_name    string         comment '用户昵称',
	passwd       string         comment '用户密码',
	name         string         comment '用户姓名',
	phone_num    string         comment '手机号',
	email        string         comment '邮箱',
	head_img     string         comment '头像',
	user_level   string         comment '用户级别',
	birthday     string         comment '用户生日',
	gender       string         comment '性别 M男,F女',
	create_time  string         comment '创建时间',
	operate_time string         comment '修改时间',
	status       string         comment '状态',
) comment '用户详细信息日增表'
    partitioned by (dt string)
    row format delimited fields terminated by '\t' null defined as '';