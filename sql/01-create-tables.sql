CREATE SCHEMA IF NOT EXISTS ods;
CREATE SCHEMA IF NOT EXISTS dwd;
CREATE SCHEMA IF NOT EXISTS dws;
CREATE SCHEMA IF NOT EXISTS ads;

-- =============================================
-- ODS 贴源层
-- =============================================

CREATE TABLE IF NOT EXISTS ods.ods_order_info (
    order_id            VARCHAR(64) PRIMARY KEY,
    user_id             VARCHAR(16),
    store_id            VARCHAR(8),
    product_id          VARCHAR(8),
    order_date          VARCHAR(20),
    order_hour          INT,
    quantity            INT,
    unit_price          DECIMAL(10,2),
    total_amount        DECIMAL(12,2),
    discount_amount     DECIMAL(10,2),
    pay_amount          DECIMAL(12,2),
    channel             VARCHAR(20),
    status              VARCHAR(20),
    delivery_type       VARCHAR(20),
    delivery_duration_min INT,
    etl_time            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS ods.ods_inventory_snapshot (
    snapshot_date   VARCHAR(20),
    store_id        VARCHAR(8),
    product_id      VARCHAR(8),
    stock_qty       INT,
    safety_stock    INT,
    reorder_point   INT,
    waste_qty       INT,
    waste_reason    VARCHAR(20),
    promotion_flag  SMALLINT,
    etl_time        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (snapshot_date, store_id, product_id)
);

CREATE TABLE IF NOT EXISTS ods.ods_user_behavior (
    event_id        VARCHAR(64) PRIMARY KEY,
    user_id         VARCHAR(16),
    product_id      VARCHAR(8),
    action          VARCHAR(20),
    event_time      VARCHAR(30),
    session_id      VARCHAR(32),
    stay_seconds    INT,
    page            VARCHAR(20),
    etl_time        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================
-- DIM 维度层
-- =============================================

CREATE TABLE IF NOT EXISTS ods.dim_product (
    product_id      VARCHAR(8) PRIMARY KEY,
    product_name    VARCHAR(50),
    category        VARCHAR(20),
    base_price      DECIMAL(10,2),
    shelf_life_days INT,
    supplier        VARCHAR(30),
    origin          VARCHAR(20),
    storage_type    VARCHAR(10),
    unit            VARCHAR(10)
);

CREATE TABLE IF NOT EXISTS ods.dim_store (
    store_id        VARCHAR(8) PRIMARY KEY,
    store_name      VARCHAR(50),
    city            VARCHAR(20),
    district        VARCHAR(30),
    area_sqm        INT,
    opening_date    DATE
);

CREATE TABLE IF NOT EXISTS ods.dim_user (
    user_id         VARCHAR(16) PRIMARY KEY,
    user_name       VARCHAR(30),
    gender          VARCHAR(4),
    age             INT,
    city            VARCHAR(20),
    membership      VARCHAR(20),
    register_date   DATE,
    user_tag        VARCHAR(20),
    lifetime_value  DECIMAL(12,2)
);

-- =============================================
-- DWD 明细层
-- =============================================

CREATE TABLE IF NOT EXISTS dwd.dwd_order_detail (
    order_id            VARCHAR(64) PRIMARY KEY,
    user_id             VARCHAR(16),
    store_id            VARCHAR(8),
    product_id          VARCHAR(8),
    order_date          DATE,
    order_hour          INT,
    order_dayofweek     INT,
    order_month         INT,
    quantity            INT,
    unit_price          DECIMAL(10,2),
    total_amount        DECIMAL(12,2),
    discount_amount     DECIMAL(10,2),
    pay_amount          DECIMAL(12,2),
    discount_rate       DECIMAL(5,3),
    channel             VARCHAR(20),
    status              VARCHAR(20),
    delivery_type       VARCHAR(20),
    delivery_duration_min INT,
    etl_time            TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS dwd.dwd_inventory_detail (
    snapshot_date   DATE,
    store_id        VARCHAR(8),
    product_id      VARCHAR(8),
    stock_qty       INT,
    safety_stock    INT,
    reorder_point   INT,
    waste_qty       INT,
    waste_reason    VARCHAR(20),
    promotion_flag  SMALLINT,
    is_understock   BOOLEAN DEFAULT FALSE,
    stock_turnover_ratio DECIMAL(8,2),
    etl_time        TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (snapshot_date, store_id, product_id)
);

CREATE TABLE IF NOT EXISTS dwd.dwd_user_behavior (
    event_id        VARCHAR(64) PRIMARY KEY,
    user_id         VARCHAR(16),
    product_id      VARCHAR(8),
    action          VARCHAR(20),
    event_datetime  TIMESTAMP,
    event_date      DATE,
    event_hour      INT,
    session_id      VARCHAR(32),
    stay_seconds    INT,
    page            VARCHAR(20),
    is_converted    BOOLEAN DEFAULT FALSE,
    etl_time        TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- =============================================
-- DWS 汇总层
-- =============================================

CREATE TABLE IF NOT EXISTS dws.dws_category_sales_day (
    category        VARCHAR(20),
    order_date      DATE,
    total_gmv       DECIMAL(16,2),
    order_count     INT,
    product_count   INT,
    user_count      INT,
    avg_order_value DECIMAL(10,2),
    discount_rate   DECIMAL(5,3),
    PRIMARY KEY (category, order_date)
);

CREATE TABLE IF NOT EXISTS dws.dws_store_sales_day (
    store_id        VARCHAR(8),
    order_date      DATE,
    total_gmv       DECIMAL(16,2),
    order_count     INT,
    user_count      INT,
    online_ratio    DECIMAL(5,3),
    delivery_avg_min DECIMAL(6,1),
    PRIMARY KEY (store_id, order_date)
);

CREATE TABLE IF NOT EXISTS dws.dws_product_sales_day (
    product_id      VARCHAR(8),
    order_date      DATE,
    sales_qty       INT,
    total_gmv       DECIMAL(14,2),
    promotion_ratio DECIMAL(5,3),
    waste_qty       INT,
    stock_qty       INT,
    PRIMARY KEY (product_id, order_date)
);

CREATE TABLE IF NOT EXISTS dws.dws_user_summary_month (
    user_id         VARCHAR(16),
    order_month     VARCHAR(7),
    order_count     INT,
    total_spend     DECIMAL(14,2),
    avg_order_value DECIMAL(10,2),
    favorite_category VARCHAR(20),
    total_views     INT,
    cart_count      INT,
    conversion_rate DECIMAL(5,3),
    PRIMARY KEY (user_id, order_month)
);

-- =============================================
-- ADS 应用层
-- =============================================

CREATE TABLE IF NOT EXISTS ads.ads_daily_kpi (
    report_date     DATE PRIMARY KEY,
    total_gmv       DECIMAL(18,2),
    total_orders    INT,
    total_users     INT,
    avg_order_value DECIMAL(10,2),
    new_users       INT,
    active_users    INT,
    repurchase_rate DECIMAL(5,3),
    online_ratio    DECIMAL(5,3)
);

CREATE TABLE IF NOT EXISTS ads.ads_category_ranking (
    report_date     DATE,
    category        VARCHAR(20),
    rank_no         INT,
    gmv             DECIMAL(16,2),
    gmv_growth_rate DECIMAL(6,3),
    gmv_share       DECIMAL(5,3),
    PRIMARY KEY (report_date, category)
);

CREATE TABLE IF NOT EXISTS ads.ads_inventory_alert (
    snapshot_date   DATE,
    store_id        VARCHAR(8),
    product_id      VARCHAR(8),
    alert_level     VARCHAR(10),
    stock_qty       INT,
    safety_stock    INT,
    suggested_order INT,
    days_to_expire  INT,
    waste_risk      VARCHAR(10),
    PRIMARY KEY (snapshot_date, store_id, product_id)
);

CREATE TABLE IF NOT EXISTS ads.ads_user_segment_report (
    as_of_date      DATE,
    user_tag        VARCHAR(20),
    user_count      INT,
    avg_spend       DECIMAL(10,2),
    avg_frequency   DECIMAL(5,1),
    active_rate     DECIMAL(5,3),
    PRIMARY KEY (as_of_date, user_tag)
);

CREATE TABLE IF NOT EXISTS ads.ads_sales_forecast (
    forecast_date   DATE,
    product_id      VARCHAR(8),
    category        VARCHAR(20),
    predicted_qty   DECIMAL(10,1),
    lower_bound     DECIMAL(10,1),
    upper_bound     DECIMAL(10,1),
    model_version   VARCHAR(20),
    forecast_time   TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (forecast_date, product_id)
);

CREATE TABLE IF NOT EXISTS ads.ads_inventory_optimization (
    product_id      VARCHAR(8) PRIMARY KEY,
    category        VARCHAR(20),
    optimal_stock   DECIMAL(10,1),
    reorder_point   DECIMAL(10,1),
    safety_stock    DECIMAL(10,1),
    eoq             DECIMAL(10,1),
    holding_cost    DECIMAL(8,2),
    shortage_cost   DECIMAL(8,2),
    total_cost      DECIMAL(10,2),
    update_time     TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
