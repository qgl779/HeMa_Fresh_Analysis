# 盒马生鲜数据分析平台
# 该项目为模拟项目

> 基于 **Hadoop HDFS + PySpark + PostgreSQL + FineBI** 构建的盒马式生鲜零售数据分析平台  
> 实现 **销量预测 · 库存优化 · 用户行为分析** 三大核心能力

---

## 📌 数据集说明

> ⚠️ 盒马鲜生（Hema）属于阿里巴巴旗下新零售品牌，其运营数据为商业机密，**无公开数据集**。  
> 本项目基于对盒马商业模式和生鲜零售行业的深入理解，构建了一套**高度逼真的模拟数据集**。

| 数据表 | 记录数 | 说明 |
|--------|--------|------|
| `dim_product` | ~150 | 10大品类 × 约15个SKU（含保质期、供应商、产地、储存方式） |
| `dim_store` | 27 | 盒马鲜生全国10城27店（上海5、北京4、深圳3 等） |
| `dim_user` | 10,000 | 含会员等级（普通/黄金/钻石/X会员）、用户标签、画像 |
| `fact_order` | 500,000 | 2024全年订单，含线上APP/小程序/线下门店三渠道 |
| `fact_inventory` | ~5M | 每日各门店各产品库存快照（含损耗记录） |
| `fact_user_behavior` | 1,500,000 | 用户浏览/加购/收藏/搜索等7类行为事件 |

### 数据特征设计亮点

- **季节性波动**：1月年货季、7-8月夏季旺季、12月双十二+年末促销
- **周末效应**：周五/周六销量权重 1.2~1.25x
- **多渠道**：线上-APP(45%)、线上-小程序(25%)、线下-门店(30%)
- **会员分层**：普通(45%)、黄金(30%)、钻石(18%)、X会员(7%)
- **生鲜损耗**：蔬菜/水果保质期3-7天，模拟过期/损坏/滞销损耗
- **动态定价**：结合季节因子 × 周末因子 × 随机波动

---

## 🏗️ 系统架构

```
┌──────────────────────────────────────────────────────────┐
│                      数据源层                              │
│  ┌─────────────────────────────────────────────────────┐ │
│  │           generate_hema_data.py                     │ │
│  │   生成6张CSV (产品/门店/用户/订单/库存/行为)          │ │
│  └───────────────────────┬─────────────────────────────┘ │
└──────────────────────────┼──────────────────────────────┘
                           │ CSV → HDFS 上传
                           ▼
┌──────────────────────────────────────────────────────────┐
│                   Hadoop HDFS                             │
│           hdfs://namenode:9000/hema_fresh/               │
└──────────────────────────┬───────────────────────────────┘
                           │ PySpark 读取
                           ▼
┌──────────────────────────────────────────────────────────┐
│                    PySpark 计算层                          │
│                                                          │
│  ┌─────────────────────────────────────────────────┐     │
│  │  01_data_cleaning.py      ETL 数据清洗            │     │
│  │  ├─ DWD 层：类型转换 / 去重 / 数据标准化          │     │
│  │  ├─ 订单清洗：date→order_date,dayofweek,month    │     │
│  │  ├─ 库存清洗：欠库存标识 / 周转率计算             │     │
│  │  └─ 行为清洗：时间戳解析 / event_date/event_hour │     │
│  └─────────────────────────────────────────────────┘     │
│                          ▼                                │
│  ┌─────────────────────────────────────────────────┐     │
│  │  02_feature_engineering.py  特征工程              │     │
│  │  ├─ 销量特征：lag_1/7/14/30, 滚动均值7/14/30天   │     │
│  │  ├─ 库存特征：库存变化率 / 损耗率 / 积压标识      │     │
│  │  └─ 用户特征：F/R/M / 复购率 / 客单价             │     │
│  └─────────────────────────────────────────────────┘     │
│                          ▼                                │
│  ┌────────────┬────────────────┬──────────────────────┐  │
│  │ 03_sales_  │ 04_inventory_  │ 05_user_behavior_    │  │
│  │ prediction │ optimization   │ analysis             │  │
│  │ .py        │ .py            │ .py                  │  │
│  │            │                │                      │  │
│  │RandomForest│ 安全库存计算    │ RFM 用户分层         │  │
│  │ + GBT 回归 │ EOQ经济订货量  │ 漏斗转化分析         │  │
│  │ 未来7天预测 │ 库存预警告警   │ 品类偏好分析         │  │
│  │            │ 损耗率趋势     │ 会员价值分析         │  │
│  └────────────┴────────────────┴──────────────────────┘  │
└──────────────────────────┬───────────────────────────────┘
                           │ JDBC 写入 / CSV 导出
                           ▼
┌──────────────────────────────────────────────────────────┐
│                   PostgreSQL 数据仓库                      │
│                                                          │
│   ODS → DWD (明细) → DWS (汇总) → ADS (应用指标)         │
│   16张表 + 5个分析视图 (RFM / 损耗 / 促销 / 库存预警)      │
└──────────────────────────┬───────────────────────────────┘
                           │ JDBC 连接
                           ▼
┌──────────────────────────────────────────────────────────┐
│                    FineBI 可视化看板                        │
│                                                          │
│   ┌─────────┐  ┌──────────┐  ┌──────────┐  ┌─────────┐  │
│   │ KPI仪表盘│  │ 销售趋势  │  │ 库存预警  │  │用户画像 │  │
│   │ GMV/订单 │  │ 品类排名  │  │ 损耗分析  │  │RFM分层  │  │
│   │ 用户/均价│  │ 门店地图  │  │ 缺货监控  │  │转化漏斗  │  │
│   └─────────┘  └──────────┘  └──────────┘  └─────────┘  │
└──────────────────────────────────────────────────────────┘
```

---

## 📁 项目目录结构

```
hema-fresh-analysis/
├── config/
│   ├── settings.py                  # 全局配置（DB/Spark/HDFS/商品/门店/品类）
│   └── requirements.txt             # Python 依赖
├── data/
│   ├── generate_hema_data.py        # ★ 数据集生成脚本（6张CSV）
│   ├── raw/                         # 原始 CSV 数据目录
│   ├── processed/                   # Parquet 清洗后数据
│   └── features/                    # 特征工程输出
├── spark/
│   ├── 01_data_cleaning.py           # PySpark ETL 数据清洗
│   ├── 02_feature_engineering.py     # 特征工程（销量/库存/用户特征）
│   ├── 03_sales_prediction.py        # 销量预测（RandomForest + GBT）
│   ├── 04_inventory_optimization.py  # 库存优化（安全库存/EOQ/预警）
│   └── 05_user_behavior_analysis.py  # 用户行为分析（RFM/漏斗/偏好）
├── sql/
│   ├── 01-create-tables.sql         # PostgreSQL 建表 DDL（4层16表）
│   └── 02-analysis-queries.sql      # 分析查询 SQL（7个核心查询+5个视图）
├── models/                          # 训练好的ML模型保存目录
├── finebi/                          # FineBI 看板配置文件
├── run_all.py                       # ★ 一键执行全部脚本
└── README.md
```

---

## 🚀 快速开始

### 环境要求

| 组件 | 版本 | 说明 |
|------|------|------|
| Python | 3.9+ | 建议 3.10 |
| Java | 8/11 | PySpark 依赖 |
| PySpark | 3.5+ | 分布式计算引擎 |
| Hadoop | 3.3+ | HDFS 存储（可选，支持 Local 模式） |
| PostgreSQL | 13+ | 数据仓库存储 |
| FineBI | 6.0+ | 可视化看板 |

### 第一步：安装依赖

```bash
cd hema-fresh-analysis
pip install -r config/requirements.txt
```

### 第二步：初始化 PostgreSQL

```bash
# 创建数据库
psql -U postgres -c "CREATE DATABASE hema_fresh_dw;"
psql -U postgres -c "CREATE USER hema_admin WITH PASSWORD 'hema2024';"
psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE hema_fresh_dw TO hema_admin;"

# 建表
psql -U hema_admin -d hema_fresh_dw -f sql/01-create-tables.sql
```

### 第三步：一键运行全流程

```bash
python run_all.py
```

或分步执行：

```bash
# Step 1: 生成模拟数据集
python data/generate_hema_data.py

# Step 2: PySpark ETL 数据清洗
spark-submit spark/01_data_cleaning.py

# Step 3: 特征工程
spark-submit spark/02_feature_engineering.py

# Step 4: 销量预测模型
spark-submit spark/03_sales_prediction.py

# Step 5: 库存优化
spark-submit spark/04_inventory_optimization.py

# Step 6: 用户行为分析
spark-submit spark/05_user_behavior_analysis.py
```

### HDFS 模式运行（可选）

```bash
# 上传数据到 HDFS
hdfs dfs -mkdir -p /hema_fresh/raw
hdfs dfs -put data/raw/*.csv /hema_fresh/raw/

# 以 YARN 模式提交
spark-submit --master yarn --deploy-mode cluster \
    --executor-memory 4g --num-executors 4 \
    spark/01_data_cleaning.py
```

---

## 📊 分析能力详解

### 1. 销量预测 (03_sales_prediction.py)

| 模型 | 说明 | 适用场景 |
|------|------|---------|
| RandomForest | 100棵树，maxDepth=10 | 高维度特征，非线性关系 |
| GBT | 100次迭代，maxDepth=8 | 时序数据，梯度增强 |

**核心特征**：
- 时序滞后特征：`sales_lag_1`, `sales_lag_7`, `sales_lag_14`, `sales_lag_30`
- 滚动统计特征：`sales_rolling_7d/14d/30d_avg`
- 时间特征：`dayofweek`, `month`, `weekofyear`
- 业务特征：`daily_gmv`, `order_count`, `user_count`, `avg_discount_rate`

**评估指标**：RMSE, MAE, R², MAPE

**输出**：未来7天按SKU的销量预测 → 写入 `ads.ads_sales_forecast`

### 2. 库存优化 (04_inventory_optimization.py)

| 模型/方法 | 公式 | 说明 |
|-----------|------|------|
| 安全库存 | `σ × z × √L` | 基于需求标准差 + 服务水平系数 1.65 |
| 再订货点 | `μ × L + σ × z` | 提前期内需求 + 安全库存 |
| EOQ | `√(2DS / H)` | 经济订货批量（最小化总成本） |

**预警分级**：
- 🔴 **严重缺货**：库存 ≤ 安全库存 × 0.5
- 🟡 **预警**：库存 ≤ 安全库存
- 🟠 **超量库存**：库存 > 最优库存 × 1.5
- 🟢 **正常**

**输出**：`ads.ads_inventory_alert`，可对接 FineBI 实时监控看板

### 3. 用户行为分析 (05_user_behavior_analysis.py)

#### 3.1 RFM 用户分层

| 分层 | R(最近购买) | F(购买频次) | M(消费金额) |
|------|------------|------------|------------|
| 高价值用户 | 近 | 高 | 高 |
| 活跃用户 | 近 | 高 | 低 |
| 沉睡高价值 | 远 | 高 | 高 |
| 新用户 | 近 | 低 | 低 |
| 流失高价值 | 远 | 低 | 高 |

#### 3.2 转化漏斗

```
浏览(view) → 点击横幅(click_banner) → 查看详情(view_detail)
→ 加购(cart) → 收藏(favorite) → 分享(share) → 搜索(search)
```

#### 3.3 品类偏好

- 用户首选品类分布
- 品类复购率排名
- 会员等级 × 消费特征交叉分析

---

## 📈 FineBI 看板设计

| 看板名称 | 数据来源 | 图表类型 | 刷新频率 |
|----------|---------|---------|---------|
| **每日KPI仪表盘** | `ads.ads_daily_kpi` | 指标卡 + 趋势折线图 | 每日 |
| **品类销售排名** | `ads.ads_category_ranking` | 条形图 + 旭日图 | 每日 |
| **门店销售地图** | `dws.dws_store_sales_day` | 中国地图 + 柱状图 | 每日 |
| **库存实时预警** | `ads.v_inventory_alert_monitor` | 表格 + 指标卡（红黄灯） | 实时 |
| **用户RFM画像** | `ads.v_user_rfm_segments` | 饼图 + 散点图 | 每周 |
| **生鲜损耗分析** | `ads.v_waste_analysis` | 折线图（分品类） | 每日 |
| **促销效果看板** | `ads.v_promotion_effect` | 对比柱状图 | 每周 |
| **销量预测看板** | `ads.ads_sales_forecast` | 预测区间折线图 | 每日 |

### FineBI 连接配置

```
数据源类型: PostgreSQL
主机: 127.0.0.1
端口: 5432
数据库: hema_fresh_dw
用户名: hema_admin
密码: hema2024
```

---

## 🗄️ 数据库分层设计

| 分层 | Schema | 表数量 | 说明 |
|------|--------|--------|------|
| ODS | `ods` | 3 | 贴源层（订单/库存/行为） |
| DWD | `dwd` | 6 | 明细层（维度表3张 + 事实表3张） |
| DWS | `dws` | 4 | 汇总层（品类/门店/产品日度 + 用户月度） |
| ADS | `ads` | 6 表 + 5视图 | 应用层（KPI/排名/预警/画像/预测/优化） |

---

## 🔧 配置说明

编辑 `config/settings.py` 修改：

```python
# 数据库
DB_CONFIG = {
    "host": "127.0.0.1",
    "port": 5432,
    "user": "hema_admin",
    "password": "hema2024",
    "database": "hema_fresh_dw"
}

# Spark
SPARK_CONFIG = {
    "app_name": "HemaFreshAnalysis",
    "master": "local[*]",           # 本地模式；集群用 "yarn"
    "spark.executor.memory": "4g",
    "spark.driver.memory": "2g"
}

# HDFS 路径（集群模式）
HDFS_BASE_PATH = "hdfs://namenode:9000/hema_fresh"
```

---

## 📝 技术选型理由

| 技术 | 选型原因 |
|------|---------|
| **PySpark** | 处理百万级数据，MLlib 提供 RF/GBT 分布式训练 |
| **PostgreSQL** | 成熟的关系型数据仓库，支持复杂SQL分析、窗口函数 |
| **Hadoop HDFS** | 大文件分布式存储，与 Spark 生态无缝集成 |
| **FineBI** | 国产自助式BI工具，支持直连数据库、拖拽式看板搭建 |
| **RandomForest** | 处理高维特征 + 非线性关系 + 天然抗过拟合 |
| **GBT** | 梯度提升树，时序预测中表现优异 |
| **EOQ模型** | 经典库存优化理论，适用于生鲜品类的定期订货场景 |

---

## ⚙️ 扩展建议

1. **实时特征**：对接 Kafka 流处理，实现实时库存监控
2. **深度学习**：引入 LSTM/Transformer 处理长序列时序预测
3. **AB测试**：基于预测结果自动触发促销策略
4. **供应链优化**：扩展至供应商管理、物流路径优化
5. **推荐系统**：基于用户行为数据构建协同过滤推荐
6. **Docker部署**：容器化全栈环境，一键部署
7. **Airflow调度**：替换 run_all.py，实现DAG任务依赖编排
