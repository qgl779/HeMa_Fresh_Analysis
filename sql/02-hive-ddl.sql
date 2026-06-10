-- ========================================================
-- Hive 数仓 DDL - hema_fresh (盒马生鲜)
-- 分层：ODS / DWD / DWS
-- ========================================================

-- ---------- 1. 创建数据库 ----------
CREATE DATABASE IF NOT EXISTS hema_fresh
COMMENT '盒马生鲜数仓'
LOCATION '/user/hive/warehouse/hema_fresh.db';

USE hema_fresh;

-- ========================================================
-- 2. ODS 层 - 原始贴源层（外部表，Parquet）
-- ========================================================

-- ---------- 订单信息 ----------
DROP TABLE IF EXISTS ods_order_info;
CREATE EXTERNAL TABLE ods_order_info (
    order_id       STRING,
    user_id        STRING,
    product_id     STRING,
    store_id       STRING,
    order_date     STRING,
    quantity       INT,
    unit_price     DECIMAL(10,2),
    total_amount   DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    pay_amount     DECIMAL(10,2),
    pay_method     STRING,
    order_status   STRING,
    channel        STRING,
    create_time    STRING
)
STORED AS PARQUET
LOCATION 'hdfs://192.168.10.128:9000/hema_fresh/ods_raw/order_info'
TBLPROPERTIES('external'='true');

-- ---------- 库存快照 ----------
DROP TABLE IF EXISTS ods_inventory_snapshot;
CREATE EXTERNAL TABLE ods_inventory_snapshot (
    snapshot_id   STRING,
    store_id      STRING,
    product_id    STRING,
    snapshot_date STRING,
    stock_qty     INT,
    safety_stock  INT,
    reorder_point INT,
    waste_qty     INT,
    inbound_qty   INT,
    outbound_qty  INT,
    unit_cost     DECIMAL(10,2),
    currency      STRING
)
STORED AS PARQUET
LOCATION 'hdfs://192.168.10.128:9000/hema_fresh/ods_raw/inventory_snapshot'
TBLPROPERTIES('external'='true');

-- ---------- 用户行为 ----------
DROP TABLE IF EXISTS ods_user_behavior;
CREATE EXTERNAL TABLE ods_user_behavior (
    event_id     STRING,
    user_id      STRING,
    product_id   STRING,
    store_id     STRING,
    event_type   STRING,
    event_time   STRING,
    page         STRING,
    device       STRING,
    session_id   STRING,
    duration_sec INT,
    is_converted INT
)
STORED AS PARQUET
LOCATION 'hdfs://192.168.10.128:9000/hema_fresh/ods_raw/user_behavior'
TBLPROPERTIES('external'='true');

-- ---------- 商品维度 ----------
DROP TABLE IF EXISTS dim_product;
CREATE EXTERNAL TABLE dim_product (
    product_id     STRING,
    product_name   STRING,
    category       STRING,
    sub_category   STRING,
    brand          STRING,
    base_price     DECIMAL(10,2),
    shelf_life_days INT,
    unit           STRING,
    supplier       STRING,
    create_time    STRING
)
STORED AS PARQUET
LOCATION 'hdfs://192.168.10.128:9000/hema_fresh/ods_raw/dim_product'
TBLPROPERTIES('external'='true');

-- ---------- 门店维度 ----------
DROP TABLE IF EXISTS dim_store;
CREATE EXTERNAL TABLE dim_store (
    store_id   STRING,
    store_name STRING,
    city       STRING,
    district   STRING,
    address    STRING,
    store_type STRING,
    area_size  DECIMAL(10,2),
    open_date  STRING,
    manager    STRING,
    phone      STRING
)
STORED AS PARQUET
LOCATION 'hdfs://192.168.10.128:9000/hema_fresh/ods_raw/dim_store'
TBLPROPERTIES('external'='true');

-- ---------- 用户维度 ----------
DROP TABLE IF EXISTS dim_user;
CREATE EXTERNAL TABLE dim_user (
    user_id         STRING,
    user_name       STRING,
    gender          STRING,
    age             INT,
    city            STRING,
    membership      STRING,
    register_date   STRING,
    user_tag        STRING,
    avg_order_value DECIMAL(10,2),
    total_orders    INT
)
STORED AS PARQUET
LOCATION 'hdfs://192.168.10.128:9000/hema_fresh/ods_raw/dim_user'
TBLPROPERTIES('external'='true');

-- ========================================================
-- 3. DWD 层 - 明细层（内表 ORC，按 dt 分区）
-- ========================================================

-- ---------- 订单明细 ----------
DROP TABLE IF EXISTS dwd_order_detail;
CREATE TABLE dwd_order_detail (
    order_id       STRING,
    user_id        BIGINT,
    product_id     BIGINT,
    store_id       BIGINT,
    order_date     STRING,
    quantity       INT,
    unit_price     DECIMAL(10,2),
    total_amount   DECIMAL(10,2),
    discount_amount DECIMAL(10,2),
    pay_amount     DECIMAL(10,2),
    pay_method     STRING,
    order_status   STRING,
    channel        STRING,
    create_time    STRING
)
PARTITIONED BY (dt STRING)
STORED AS ORC;

-- ---------- 库存明细 ----------
DROP TABLE IF EXISTS dwd_inventory_detail;
CREATE TABLE dwd_inventory_detail (
    snapshot_id   STRING,
    store_id      BIGINT,
    product_id    BIGINT,
    snapshot_date STRING,
    stock_qty     INT,
    safety_stock  INT,
    reorder_point INT,
    waste_qty     INT,
    inbound_qty   INT,
    outbound_qty  INT,
    unit_cost     DECIMAL(10,2),
    currency      STRING
)
PARTITIONED BY (dt STRING)
STORED AS ORC;

-- ---------- 用户行为明细 ----------
DROP TABLE IF EXISTS dwd_user_behavior;
CREATE TABLE dwd_user_behavior (
    event_id     STRING,
    user_id      BIGINT,
    product_id   BIGINT,
    store_id     BIGINT,
    event_type   STRING,
    event_time   STRING,
    page         STRING,
    device       STRING,
    session_id   STRING,
    duration_sec INT,
    is_converted INT
)
PARTITIONED BY (dt STRING)
STORED AS ORC;

-- ========================================================
-- 4. DWS 层 - 汇总层（内表 ORC，聚合宽表）
-- ========================================================

-- ---------- 每日销售汇总宽表 ----------
DROP TABLE IF EXISTS dws_sales_daily;
CREATE TABLE dws_sales_daily (
    dt             STRING,
    product_id     BIGINT,
    category       STRING,
    city           STRING,
    sales_qty      BIGINT,
    sales_amount   DECIMAL(18,2),
    pay_amount     DECIMAL(18,2),
    discount_amount DECIMAL(18,2),
    order_count    BIGINT,
    user_count     BIGINT
)
COMMENT '每日销售汇总宽表'
STORED AS ORC;

-- ---------- 每日库存宽表 ----------
DROP TABLE IF EXISTS dws_inventory_daily;
CREATE TABLE dws_inventory_daily (
    dt              STRING,
    product_id      BIGINT,
    category        STRING,
    store_id        BIGINT,
    city            STRING,
    stock_qty       BIGINT,
    safety_stock    BIGINT,
    waste_qty       BIGINT,
    waste_rate      DECIMAL(10,4),
    stock_turnover  DECIMAL(10,4),
    understock_flag INT,
    overstock_flag  INT
)
COMMENT '每日库存宽表'
STORED AS ORC;

-- ---------- 用户 RFM 分层 ----------
DROP TABLE IF EXISTS dws_user_rfm;
CREATE TABLE dws_user_rfm (
    user_id         BIGINT,
    r_score         INT,
    f_score         INT,
    m_score         INT,
    rfm_segment     STRING,
    recency_days    INT,
    purchase_count  INT,
    total_spend     DECIMAL(18,2),
    avg_order_value DECIMAL(18,2),
    last_order_date STRING,
    first_order_date STRING
)
COMMENT '用户 RFM 分层'
STORED AS ORC;

-- ---------- 品类销售排名 ----------
DROP TABLE IF EXISTS dws_category_ranking;
CREATE TABLE dws_category_ranking (
    dt           STRING,
    category     STRING,
    product_id   BIGINT,
    city         STRING,
    sales_qty    BIGINT,
    sales_amount DECIMAL(18,2),
    qty_rank     INT,
    amount_rank  INT
)
COMMENT '品类销售排名'
STORED AS ORC;

-- ---------- 会员等级贡献 ----------
DROP TABLE IF EXISTS dws_membership_contribution;
CREATE TABLE dws_membership_contribution (
    dt              STRING,
    membership_level STRING,
    user_count      BIGINT,
    total_orders    BIGINT,
    total_spend     DECIMAL(18,2),
    avg_order_value DECIMAL(18,2),
    pay_ratio       DECIMAL(10,4)
)
COMMENT '会员等级贡献'
STORED AS ORC;

-- ========================================================
-- DDL 完成
-- ========================================================
