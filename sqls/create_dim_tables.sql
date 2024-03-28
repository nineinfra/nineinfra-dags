use datahouse;

-- -------------------------------------------------------------------------------------------------
-- DIM 层建表语句
-- -------------------------------------------------------------------------------------------------
-- 商品维度表
drop table if exists dim_sku_full;
create table  if not exists dim_sku_full
(
    id                   string                                                              comment 'SKU_ID',
    price                decimal(16, 2)                                                      comment '商品价格',
    sku_name             string                                                              comment '商品名称',
    sku_desc             string                                                              comment '商品描述',
    weight               decimal(16, 2)                                                      comment '重量',
    is_sale              boolean                                                             comment '是否在售',
    spu_id               string                                                              comment 'SPU 编号',
    spu_name             string                                                              comment 'SPU 名称',
    category3_id         string                                                              comment '三级分类 ID',
    category3_name       string                                                              comment '三级分类名称',
    category2_id         string                                                              comment '二级分类 ID',
    category2_name       string                                                              comment '二级分类名称',
    category1_id         string                                                              comment '一级分类 ID',
    category1_name       string                                                              comment '一级分类名称',
    tm_id                string                                                              comment '品牌 ID',
    tm_name              string                                                              comment '品牌名称',
    sku_attr_values      array<struct<attr_id: string,        value_id: string,
                                      attr_name: string,      value_name: string>>           comment '平台属性',
    sku_sale_attr_values array<struct<sale_attr_id: string,   sale_attr_value_id: string,
                                      sale_attr_name: string, sale_attr_value_name: string>> comment '销售属性',
    create_time          string                                                              comment '创建时间'
) comment '商品维度表'
    partitioned by (dt string)
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

-- 优惠券维度表
drop table if exists dim_coupon_full;
create table  if not exists dim_coupon_full
(
    id               string         comment '购物券编号',
    coupon_name      string         comment '购物券名称',
    coupon_type_code string         comment '购物券类型编码',
    coupon_type_name string         comment '购物券类型名称',
    condition_amount decimal(16, 2) comment '满额数',
    condition_num    bigint         comment '满件数',
    activity_id      string         comment '活动编号',
    benefit_amount   decimal(16, 2) comment '减金额',
    benefit_discount decimal(16, 2) comment '折扣',
    benefit_rule     string         comment '优惠规则:满元 * 减 * 元，满 * 件打 * 折',
    create_time      string         comment '创建时间',
    range_type_code  string         comment '优惠范围类型编码',
    range_type_name  string         comment '优惠范围类型名称',
    limit_num        bigint         comment '最多领取次数',
    taken_count      bigint         comment '已领取次数',
    start_time       string         comment '可以领取的开始日期',
    end_time         string         comment '可以领取的结束日期',
    operate_time     string         comment '修改时间',
    expire_time      string         comment '过期时间'
) comment '优惠券维度表'
    partitioned by (dt string)
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

-- 活动维度表
drop table if exists dim_activity_full;
create table  if not exists dim_activity_full
(
    activity_rule_id   string         comment '活动规则 ID',
    activity_id        string         comment '活动 ID',
    activity_name      string         comment '活动名称',
    activity_type_code string         comment '活动类型编码',
    activity_type_name string         comment '活动类型名称',
    activity_desc      string         comment '活动描述',
    start_time         string         comment '开始时间',
    end_time           string         comment '结束时间',
    create_time        string         comment '创建时间',
    condition_amount   decimal(16, 2) comment '满减金额',
    condition_num      bigint         comment '满减件数',
    benefit_amount     decimal(16, 2) comment '优惠金额',
    benefit_discount   decimal(16, 2) comment '优惠折扣',
    benefit_rule       string         comment '优惠规则',
    benefit_level      string         comment '优惠级别'
) comment '活动信息表'
    partitioned by (dt string)
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

-- 地区维度表
drop table if exists dim_province_full;
create table  if not exists dim_province_full
(
    id            string comment 'ID',
    province_name string comment '省市名称',
    area_code     string comment '地区编码',
    iso_code      string comment '旧版 ISO-3166-2 编码，供可视化使用',
    iso_3166_2    string comment '新版 IOS-3166-2 编码，供可视化使用',
    region_id     string comment '地区 ID',
    region_name   string comment '地区名称'
) comment '地区维度表'
    partitioned by (dt string)
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

-- 日期维度表
drop table if exists dim_date;
create table  if not exists dim_date
(
    date_id    string comment '日期 ID',
    week_id    string comment '周 ID,一年中的第几周',
    week_day   string comment '周几',
    day        string comment '每月的第几天',
    month      string comment '一年中的第几月',
    quarter    string comment '一年中的第几季度',
    year       string comment '年份',
    is_workday string comment '是否是工作日',
    holiday_id string comment '节假日'
) comment '时间维度表'
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

-- 用户维度表
drop table if exists dim_user_zip;
create table  if not exists dim_user_zip
(
    id           string comment '用户 ID',
    login_name   string comment '用户名称',
    nick_name    string comment '用户昵称',
    name         string comment '用户姓名',
    phone_num    string comment '手机号码',
    email        string comment '邮箱',
    user_level   string comment '用户等级',
    birthday     string comment '生日',
    gender       string comment '性别',
    create_time  string comment '创建时间',
    operate_time string comment '操作时间',
    start_date   string comment '开始日期',
    end_date     string comment '结束日期'
) comment '用户表'
    partitioned by (dt string)
    stored as orc
    tblproperties ('orc.compress' = 'snappy');

-- -------------------------------------------------------------------------------------------------
-- TMP 层建表语句
-- -------------------------------------------------------------------------------------------------
-- 日期维度表
drop table if exists tmp_dim_date_info;
create table  if not exists tmp_dim_date_info
(
    date_id    string comment '日',
    week_id    string comment '周 ID',
    week_day   string comment '周几',
    day        string comment '每月的第几天',
    month      string comment '第几月',
    quarter    string comment '第几季度',
    year       string comment '年',
    is_workday string comment '是否是工作日',
    holiday_id string comment '节假日'
) comment '时间维度表' row format delimited fields terminated by '\t';
