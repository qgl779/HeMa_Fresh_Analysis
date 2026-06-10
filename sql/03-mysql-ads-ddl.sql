-- ========================================================
-- MySQL ADS 层 BI 表 DDL - hema_fresh_ads
-- 用于 FineBI / 报表展示
-- ========================================================

-- ---------- 1. 创建数据库 ----------
CREATE DATABASE IF NOT EXISTS hema_fresh_ads
DEFAULT CHARACTER SET utf8mb4
COLLATE utf8mb4_unicode_ci;

USE hema_fresh_ads;

-- ========================================================
-- 2. ADS 层 BI 表
-- ========================================================

-- ---------- 销量 7 天预测 ----------
DROP TABLE IF EXISTS ads_sales_forecast;
CREATE TABLE ads_sales_forecast (
    forecast_id     BIGINT UNSIGNED NOT NULL AUTO_INCREMENT COMMENT '预测ID',
    dt              DATE NOT NULL COMMENT '预测日期',
    product_id      INT NOT NULL COMMENT '商品ID',
    category        VARCHAR(64) NOT NULL COMMENT '品类',
    forecast_qty    DOUBLE NOT NULL DEFAULT 0 COMMENT '预测销量',
    forecast_amount DOUBLE NOT NULL DEFAULT 0 COMMENT '预测金额',
    model_type      VARCHAR(32) NOT NULL DEFAULT 'ARIMA' COMMENT '预测模型',
    rmse            DOUBLE COMMENT 'RMSE',
    mape            DOUBLE COMMENT 'MAPE',
    create_time     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (forecast_id),
    KEY idx_dt_product (dt, product_id),
    KEY idx_category (category)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='销量7天预测';

-- ---------- 库存优化建议 ----------
DROP TABLE IF EXISTS ads_inventory_optimization;
CREATE TABLE ads_inventory_optimization (
    product_id     INT NOT NULL COMMENT '商品ID',
    category       VARCHAR(64) NOT NULL COMMENT '品类',
    product_name   VARCHAR(128) NOT NULL COMMENT '商品名称',
    current_stock  INT NOT NULL DEFAULT 0 COMMENT '当前库存',
    safety_stock   INT NOT NULL DEFAULT 0 COMMENT '安全库存',
    eoq            INT NOT NULL DEFAULT 0 COMMENT '经济订货量',
    reorder_point  INT NOT NULL DEFAULT 0 COMMENT '再订货点',
    alert_level    VARCHAR(16) NOT NULL DEFAULT 'NORMAL' COMMENT '预警等级',
    waste_rate     DOUBLE NOT NULL DEFAULT 0 COMMENT '损耗率',
    stock_turnover DOUBLE NOT NULL DEFAULT 0 COMMENT '库存周转率',
    suggestion     VARCHAR(512) COMMENT '优化建议',
    update_time    DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (product_id),
    KEY idx_category (category),
    KEY idx_alert (alert_level)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='库存优化建议';

-- ---------- 用户 RFM 分层画像 ----------
DROP TABLE IF EXISTS ads_user_segment_report;
CREATE TABLE ads_user_segment_report (
    user_id         BIGINT NOT NULL COMMENT '用户ID',
    rfm_segment     VARCHAR(32) NOT NULL COMMENT 'RFM分层',
    r_score         INT NOT NULL COMMENT 'R得分',
    f_score         INT NOT NULL COMMENT 'F得分',
    m_score         INT NOT NULL COMMENT 'M得分',
    recency_days    INT NOT NULL DEFAULT 0 COMMENT '最近购买天数',
    purchase_count  INT NOT NULL DEFAULT 0 COMMENT '购买次数',
    total_spend     DOUBLE NOT NULL DEFAULT 0 COMMENT '总消费金额',
    avg_order_value DOUBLE NOT NULL DEFAULT 0 COMMENT '平均客单价',
    last_order_date DATE COMMENT '最后下单日期',
    first_order_date DATE COMMENT '首次下单日期',
    membership_level VARCHAR(16) COMMENT '会员等级',
    update_time     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (user_id),
    KEY idx_rfm_segment (rfm_segment),
    KEY idx_membership (membership_level)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='用户RFM分层画像';

-- ---------- 每日销售总览 ----------
DROP TABLE IF EXISTS ads_daily_sales_summary;
CREATE TABLE ads_daily_sales_summary (
    dt              DATE NOT NULL COMMENT '统计日期',
    total_orders    INT NOT NULL DEFAULT 0 COMMENT '订单总数',
    total_sales     DOUBLE NOT NULL DEFAULT 0 COMMENT '销售总额',
    total_users     INT NOT NULL DEFAULT 0 COMMENT '用户数',
    avg_order_value DOUBLE NOT NULL DEFAULT 0 COMMENT '平均客单价',
    discount_rate   DOUBLE NOT NULL DEFAULT 0 COMMENT '折扣率',
    create_time     DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (dt)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='每日销售总览';

-- ---------- 品类销售排名 ----------
DROP TABLE IF EXISTS ads_category_ranking;
CREATE TABLE ads_category_ranking (
    dt           DATE NOT NULL COMMENT '统计日期',
    category     VARCHAR(64) NOT NULL COMMENT '品类',
    product_id   INT NOT NULL COMMENT '商品ID',
    city         VARCHAR(32) NOT NULL COMMENT '城市',
    sales_qty    INT NOT NULL DEFAULT 0 COMMENT '销售数量',
    sales_amount DOUBLE NOT NULL DEFAULT 0 COMMENT '销售金额',
    qty_rank     INT NOT NULL DEFAULT 0 COMMENT '数量排名',
    amount_rank  INT NOT NULL DEFAULT 0 COMMENT '金额排名',
    create_time  DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT '创建时间',
    PRIMARY KEY (dt, category, product_id),
    KEY idx_dt_city (dt, city),
    KEY idx_category (category)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='品类销售排名';

-- ---------- 会员贡献 ----------
DROP TABLE IF EXISTS ads_membership_contribution;
CREATE TABLE ads_membership_contribution (
    membership_level VARCHAR(16) NOT NULL COMMENT '会员等级',
    user_count       INT NOT NULL DEFAULT 0 COMMENT '用户数',
    total_orders     INT NOT NULL DEFAULT 0 COMMENT '订单总数',
    total_spend      DOUBLE NOT NULL DEFAULT 0 COMMENT '总消费',
    avg_order_value  DOUBLE NOT NULL DEFAULT 0 COMMENT '平均客单价',
    pay_ratio        DOUBLE NOT NULL DEFAULT 0 COMMENT '支付占比',
    update_time      DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT '更新时间',
    PRIMARY KEY (membership_level)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='会员贡献';

-- ========================================================
-- ADS 层 DDL 完成
-- ========================================================
