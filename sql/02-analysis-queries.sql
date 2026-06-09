-- =============================================
-- 盒马生鲜数据分析 SQL 查询集（FineBI 数据源）
-- =============================================

-- 1. 每日核心 KPI
-- =============================================
INSERT INTO ads.ads_daily_kpi (report_date, total_gmv, total_orders, total_users, avg_order_value, new_users, active_users, repurchase_rate, online_ratio)
SELECT
    d.order_date AS report_date,
    SUM(d.pay_amount) AS total_gmv,
    COUNT(DISTINCT d.order_id) AS total_orders,
    COUNT(DISTINCT d.user_id) AS total_users,
    ROUND(SUM(d.pay_amount) / COUNT(DISTINCT d.order_id), 2) AS avg_order_value,
    COUNT(DISTINCT CASE WHEN u.register_date >= d.order_date - INTERVAL '30 days' THEN d.user_id END) AS new_users,
    COUNT(DISTINCT d.user_id) AS active_users,
    ROUND(
        COUNT(DISTINCT CASE WHEN user_ord.order_cnt > 1 THEN d.user_id END) * 1.0
        / NULLIF(COUNT(DISTINCT d.user_id), 0), 3
    ) AS repurchase_rate,
    ROUND(
        COUNT(DISTINCT CASE WHEN d.channel LIKE '线上%' THEN d.order_id END) * 1.0
        / NULLIF(COUNT(DISTINCT d.order_id), 0), 3
    ) AS online_ratio
FROM dwd.dwd_order_detail d
LEFT JOIN dwd.dim_user u ON d.user_id = u.user_id
LEFT JOIN (
    SELECT user_id, COUNT(*) AS order_cnt
    FROM dwd.dwd_order_detail
    GROUP BY user_id
) user_ord ON d.user_id = user_ord.user_id
GROUP BY d.order_date
ON CONFLICT (report_date) DO UPDATE SET
    total_gmv = EXCLUDED.total_gmv,
    total_orders = EXCLUDED.total_orders,
    total_users = EXCLUDED.total_users,
    avg_order_value = EXCLUDED.avg_order_value;


-- 2. 品类销售排名
-- =============================================
INSERT INTO ads.ads_category_ranking (report_date, category, rank_no, gmv, gmv_growth_rate, gmv_share)
SELECT
    order_date AS report_date,
    category,
    RANK() OVER (PARTITION BY order_date ORDER BY SUM(pay_amount) DESC) AS rank_no,
    SUM(pay_amount) AS gmv,
    ROUND(
        (SUM(pay_amount) - LAG(SUM(pay_amount), 7) OVER (PARTITION BY category ORDER BY order_date))
        / NULLIF(LAG(SUM(pay_amount), 7) OVER (PARTITION BY category ORDER BY order_date), 0), 3
    ) AS gmv_growth_rate,
    ROUND(
        SUM(pay_amount) / NULLIF(SUM(SUM(pay_amount)) OVER (PARTITION BY order_date), 0), 3
    ) AS gmv_share
FROM dwd.dwd_order_detail d
JOIN dwd.dim_product p ON d.product_id = p.product_id
WHERE d.status = 'completed'
GROUP BY order_date, category
ON CONFLICT (report_date, category) DO UPDATE SET
    rank_no = EXCLUDED.rank_no,
    gmv = EXCLUDED.gmv,
    gmv_growth_rate = EXCLUDED.gmv_growth_rate,
    gmv_share = EXCLUDED.gmv_share;


-- 3. 门店销售日报（FineBI 地图可视化用）
-- =============================================
INSERT INTO dws.dws_store_sales_day (store_id, order_date, total_gmv, order_count, user_count, online_ratio, delivery_avg_min)
SELECT
    d.store_id,
    d.order_date,
    SUM(d.pay_amount) AS total_gmv,
    COUNT(DISTINCT d.order_id) AS order_count,
    COUNT(DISTINCT d.user_id) AS user_count,
    ROUND(
        COUNT(DISTINCT CASE WHEN d.channel LIKE '线上%' THEN d.order_id END) * 1.0
        / NULLIF(COUNT(DISTINCT d.order_id), 0), 3
    ) AS online_ratio,
    ROUND(AVG(CASE WHEN d.channel LIKE '线上%' THEN d.delivery_duration_min END), 1) AS delivery_avg_min
FROM dwd.dwd_order_detail d
WHERE d.status = 'completed'
GROUP BY d.store_id, d.order_date
ON CONFLICT (store_id, order_date) DO UPDATE SET
    total_gmv = EXCLUDED.total_gmv,
    order_count = EXCLUDED.order_count,
    user_count = EXCLUDED.user_count;


-- 4. 库存预警视图（FineBI 实时监控用）
-- =============================================
CREATE OR REPLACE VIEW ads.v_inventory_alert_monitor AS
SELECT
    i.snapshot_date,
    i.store_id,
    s.store_name,
    s.city,
    i.product_id,
    p.product_name,
    p.category,
    p.shelf_life_days,
    i.stock_qty,
    i.safety_stock,
    i.reorder_point,
    i.waste_qty,
    CASE
        WHEN i.stock_qty <= i.safety_stock * 0.5 THEN '严重缺货'
        WHEN i.stock_qty <= i.safety_stock THEN '低库存预警'
        WHEN i.stock_qty > i.safety_stock * 3 THEN '库存积压'
        ELSE '正常'
    END AS alert_status,
    CASE
        WHEN i.stock_qty < i.reorder_point
        THEN GREATEST(i.reorder_point * 2 - i.stock_qty, 0)
        ELSE 0
    END AS suggested_replenish
FROM dwd.dwd_inventory_detail i
JOIN dwd.dim_product p ON i.product_id = p.product_id
JOIN dwd.dim_store s ON i.store_id = s.store_id
WHERE i.snapshot_date = (SELECT MAX(snapshot_date) FROM dwd.dwd_inventory_detail);


-- 5. 用户 RFM 分层汇总视图
-- =============================================
CREATE OR REPLACE VIEW ads.v_user_rfm_segments AS
WITH user_stats AS (
    SELECT
        user_id,
        COUNT(DISTINCT order_id) AS frequency,
        SUM(pay_amount) AS monetary,
        MAX(order_date) AS last_order,
        MIN(order_date) AS first_order,
        DATEDIFF(CURRENT_DATE, MAX(order_date)) AS recency
    FROM dwd.dwd_order_detail
    WHERE status = 'completed'
    GROUP BY user_id
),
segments AS (
    SELECT
        u.*,
        CASE
            WHEN recency <= 30 AND frequency >= 10 AND monetary >= 5000 THEN '高价值用户'
            WHEN recency <= 30 AND frequency >= 5 THEN '活跃用户'
            WHEN recency > 30 AND recency <= 90 AND frequency >= 10 THEN '沉睡高价值'
            WHEN recency <= 30 AND frequency < 5 THEN '新用户'
            WHEN recency > 90 AND monetary >= 5000 THEN '流失高价值'
            WHEN recency > 90 AND frequency < 3 THEN '流失用户'
            ELSE '一般用户'
        END AS rfm_segment
    FROM user_stats u
)
SELECT
    s.*,
    du.membership,
    du.user_tag,
    du.city,
    du.age,
    du.gender
FROM segments s
JOIN dwd.dim_user du ON s.user_id = du.user_id;


-- 6. 生鲜损耗率分析（按品类×周）
-- =============================================
CREATE OR REPLACE VIEW ads.v_waste_analysis AS
SELECT
    p.category,
    DATE_TRUNC('week', i.snapshot_date)::DATE AS week_start,
    SUM(i.waste_qty) AS total_waste,
    ROUND(AVG(i.waste_qty * 1.0 / NULLIF(i.stock_qty, 0)), 4) AS waste_rate,
    COUNT(DISTINCT i.product_id) AS product_count,
    COUNT(DISTINCT i.store_id) AS store_count
FROM dwd.dwd_inventory_detail i
JOIN dwd.dim_product p ON i.product_id = p.product_id
GROUP BY p.category, DATE_TRUNC('week', i.snapshot_date)
ORDER BY week_start DESC, total_waste DESC;


-- 7. 促销效果分析
-- =============================================
CREATE OR REPLACE VIEW ads.v_promotion_effect AS
SELECT
    p.category,
    p.product_name,
    CASE WHEN i.promotion_flag = 1 THEN '促销' ELSE '非促销' END AS promo_status,
    COUNT(DISTINCT i.product_id) AS sku_count,
    ROUND(AVG(d.pay_amount), 2) AS avg_price,
    ROUND(AVG(d.quantity), 1) AS avg_quantity,
    ROUND(SUM(d.pay_amount), 2) AS total_revenue,
    ROUND(AVG(i.waste_qty), 1) AS avg_waste
FROM dwd.dwd_inventory_detail i
JOIN dwd.dim_product p ON i.product_id = p.product_id
LEFT JOIN dwd.dwd_order_detail d
    ON i.product_id = d.product_id
    AND i.snapshot_date = d.order_date
    AND i.store_id = d.store_id
WHERE i.snapshot_date >= CURRENT_DATE - INTERVAL '30 days'
GROUP BY p.category, p.product_name, i.promotion_flag
ORDER BY total_revenue DESC
LIMIT 50;
