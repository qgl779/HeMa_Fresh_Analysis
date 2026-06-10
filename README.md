# 盒马生鲜数据分析平台

> **Hadoop HDFS + Spark + Hive + PostgreSQL + MySQL + FineBI** 全栈生鲜零售分析平台  
> 四大核心：**数据治理 · 销量预测 · 库存优化 · 用户行为分析**

---

## 📐 总体架构

```
┌─────────────┐    ┌──────────────┐    ┌──────────────┐    ┌──────────┐
│  PostgreSQL  │──▶│  HDFS / Hive │──▶│  Hive DWD    │──▶│  Hive    │
│  (ods 贴源)  │    │  (ods 镜像)   │    │  (清洗明细)   │    │  DWS     │
└─────────────┘    └──────────────┘    └──────────────┘    └──────────┘
                                                               │
                                                               ▼
                                                        ┌──────────────┐
                                                        │  MySQL ADS   │
                                                        │  (BI 应用层)  │
                                                        └──────────────┘
                                                               │
                                                               ▼
                                                        ┌──────────────┐
                                                        │  FineBI 报表  │
                                                        └──────────────┘
```

- **PostgreSQL**：原始业务数据 ODS（贴源），从模拟数据生成
- **HDFS + Hive**：ODS 镜像 + DWD 清洗规范 + DWS 聚合汇总
- **MySQL**：ADS 应用层，6 张 BI 宽表，直接对接 FineBI

---

## 📌 数据集

> ⚠️ 盒马（阿里巴巴）无公开数据集。本项目构建了**高度逼真的模拟数据集**。

| 表名 | 规模 | 说明 |
|------|------|------|
| `dim_product` | ~150 行 | 10 品类 × 15 SKU（含保质期/产地/储存方式） |
| `dim_store` | 27 行 | 全国 10 城 27 店 |
| `dim_user` | 10,000 行 | 4 级会员 + 8 种用户标签 |
| `fact_order` | 500,000 行 | 2024 全年订单（APP/小程序/门店三渠道） |
| `fact_inventory` | ~500 万行 | 每日每店每产品库存快照 + 损耗 |
| `fact_user_behavior` | 150 万行 | 7 类行为事件（浏览/加购/收藏等） |

---

## 🐧 集群一键运行（推荐）

> 目标集群：Hadoop 3.3.6 + Spark 3.5.8 + Hive 3.1.2 + PostgreSQL 15 + MySQL  
> 集群详情参考：[README_CLUSTER.md](./README_CLUSTER.md)

```bash
cd /opt/project/hema-fresh-analysis
chmod +x cluster_run.sh
bash cluster_run.sh
```

脚本自动完成 **13 步**全流程：

| Step | 脚本 | 说明 |
|------|------|------|
| 1 | — | 环境检查（Java / Hadoop / Spark / Hive / PG / MySQL） |
| 2 | `data/generate_hema_data.py` | 生成模拟数据 → PG ods + 上传 HDFS |
| 3 | `sql/02-hive-ddl.sql` | Hive 建库建表（ods/dwd/dws） |
| 4 | `sql/03-mysql-ads-ddl.sql` | MySQL ADS 建表（6 张 BI 表） |
| 5 | `spark/00_extract_pg_to_hdfs.py` | PG ods → HDFS / Hive ods 层 |
| 6 | `spark/01_data_cleaning.py` | 数据清洗：ods → dwd |
| 7 | `spark/02_feature_engineering.py` | 特征聚合：dwd → dws + HDFS 中间数据集 |
| 8 | `spark/03_sales_prediction.py` | 销量预测：dws → MySQL ads_sales_forecast |
| 9 | `spark/04_inventory_optimization.py` | 库存优化：dws → MySQL ads_inventory_optimization |
| 10 | `spark/05_user_behavior_analysis.py` | 用户画像：dws → MySQL ads_user_segment_report |
| 11 | `spark/06_category_ranking.py` | 品类排名：dwd + dim_product → MySQL ads_category_ranking |
| 12 | `spark/07_ads_to_mysql.py` | DWS 汇总 → MySQL ads_daily_sales_summary + ads_membership_contribution |
| 13 | — | 结果验证：Hive 表列表 + MySQL 各表行数 |

---

## 🐧 手动分步执行

```bash
cd /opt/project/hema-fresh-analysis
export PYTHONPATH=$(pwd):$PYTHONPATH

# Step 1: 生成数据集并写入 PG + 上传 HDFS
python3 data/generate_hema_data.py --write-pg --upload-hdfs

# Step 2: 执行 Hive DDL
hive -f sql/02-hive-ddl.sql

# Step 3: 执行 MySQL ADS DDL（在 db-server 上）
mysql -h 192.168.10.144 -u hema_ads -p < sql/03-mysql-ads-ddl.sql

# Spark 公共提交参数
SPARK_OPTS="--master yarn --deploy-mode client \
  --driver-memory 2g --num-executors 3 \
  --executor-cores 2 --executor-memory 4g \
  --conf spark.driver.host=192.168.10.128 \
  --conf spark.sql.shuffle.partitions=200 \
  --conf spark.sql.adaptive.enabled=true"

# Step 4: PG → HDFS/Hive ODS
spark-submit $SPARK_OPTS spark/00_extract_pg_to_hdfs.py

# Step 5: ODS → DWD 清洗
spark-submit $SPARK_OPTS spark/01_data_cleaning.py

# Step 6: DWD → DWS 特征聚合
spark-submit $SPARK_OPTS spark/02_feature_engineering.py

# Step 7: 销量预测
spark-submit $SPARK_OPTS spark/03_sales_prediction.py

# Step 8: 库存优化
spark-submit $SPARK_OPTS spark/04_inventory_optimization.py

# Step 9: 用户画像
spark-submit $SPARK_OPTS spark/05_user_behavior_analysis.py

# Step 10: 品类排名
spark-submit $SPARK_OPTS spark/06_category_ranking.py

# Step 11: DWS → MySQL 汇总
spark-submit $SPARK_OPTS spark/07_ads_to_mysql.py
```

---

## 📂 目录结构

```
hema-fresh-analysis/
├── cluster_run.sh                      # ★ 一键全流程执行脚本（13 步）
├── README.md                           # 本文件
├── README_CLUSTER.md                   # 集群部署详细指南
├── setup.sh                            # 环境初始化脚本
├── run.sh                              # 旧版执行脚本（已废弃，保留备用）
├── run_all.py                          # Windows 全流程执行脚本
├── pg_connect_test.py                  # PG 连接校验脚本
│
├── config/
│   ├── settings.py                     # 全局配置 (DB/Spark/HDFS/品类/门店)
│   └── requirements.txt                # Python 依赖
│
├── data/
│   └── generate_hema_data.py           # 数据集生成 (6张表)
│
├── spark/
│   ├── 00_extract_pg_to_hdfs.py        # PG ods → HDFS/Hive ods 抽取
│   ├── 01_data_cleaning.py             # ODS → DWD 数据清洗
│   ├── 02_feature_engineering.py       # DWD → DWS 特征聚合
│   ├── 03_sales_prediction.py          # 销量预测 (RF + GBT → MySQL)
│   ├── 04_inventory_optimization.py    # 库存优化 (安全库存/EOQ → MySQL)
│   ├── 05_user_behavior_analysis.py    # 用户画像 (RFM/漏斗 → MySQL)
│   ├── 06_category_ranking.py          # 品类销售排名 → MySQL
│   └── 07_ads_to_mysql.py             # DWS 汇总 → MySQL (日销售+会员贡献)
│
├── sql/
│   ├── 01-create-tables.sql            # PG ODS/DWD/DWS/ADS DDL
│   ├── 02-hive-ddl.sql                 # Hive ods/dwd/dws 建表
│   ├── 03-mysql-ads-ddl.sql            # MySQL ADS 6 张 BI 表 DDL
│   └── 02-analysis-queries.sql         # 分析查询 + FineBI 视图
│
└── .workbuddy/                         # 项目工作记录（勿删）
    └── memory/
```

---

## 📊 四大分析能力

### 1. 数据治理（ODS → DWD → DWS → ADS）
- **ODS 贴源层**：6 张 PG 原始表 → HDFS Parquet → Hive 镜像表
- **DWD 清洗层**：空值填充、去重、类型规范、事实宽表构建
- **DWS 汇总层**：日/月维度聚合，20+ 业务指标（GMV、客单价、SKU 动销、渠道转化）
- **ADS 应用层**：6 张 MySQL BI 宽表，直接对接 FineBI

### 2. 销量预测
- RandomForest (100 树) + GBT (100 迭代) 双模型对比
- 时序滞后 + 滚动统计 + 时间特征（含周末因子调整）
- 评估：RMSE / MAE / R² / MAPE
- 输出：未来 7 天逐 SKU 预测 → `ads_sales_forecast`

### 3. 库存优化
- 安全库存公式：`σ × z × √L`（服务水平 z=1.65）
- 经济订货批量 EOQ：`√(2DS/H)`
- 四级预警：🔴严重缺货 / 🟡预警 / 🟠超量 / 🟢正常
- 生鲜损耗率 + 库存周转率分析 → `ads_inventory_optimization`

### 4. 用户行为
- RFM 五层用户分层（高价值/活跃/沉睡/新/流失）
- 转化漏斗（浏览→点击→详情→加购→收藏→分享）
- 品类偏好 + 复购分析 + 会员价值对比 → `ads_user_segment_report`

### 补充
- **品类排名**：按城市/品类/日期维度的销售排名 → `ads_category_ranking`
- **每日销售汇总**：日维度总览指标 → `ads_daily_sales_summary`
- **会员贡献**：各等级会员消费贡献 → `ads_membership_contribution`

---

## 🔧 FineBI 对接

### 数据源配置
```
类型: MySQL
主机: 192.168.10.144
端口: 3306
数据库: hema_fresh_ads
用户: hema_ads
密码: hema2024
```

### ADS 应用表（直接拖拽到 FineBI）

| 表名 | 用途 | 粒度 |
|------|------|------|
| `ads_sales_forecast` | 未来 7 天销量预测 | 日期 × SKU |
| `ads_inventory_optimization` | 实时库存预警 + 补货建议 | SKU |
| `ads_user_segment_report` | 用户 RFM 分层画像 | 用户 |
| `ads_daily_sales_summary` | 每日销售总览 | 日期 |
| `ads_category_ranking` | 品类销售排名 & 城市分布 | 日期 × 品类 × SKU |
| `ads_membership_contribution` | 会员等级贡献对比 | 会员等级 |

---

## 📝 数据库分层

| 层 | 存储 | 数量 | 职责 |
|----|------|------|------|
| ODS | PostgreSQL + Hive | 6 表 | 贴源层，原始数据 |
| DWD | Hive | 3 维度 + 3 事实 | 清洗标准化，类型转换 |
| DWS | Hive | 4 汇总表 | 日度/月度轻度聚合 |
| ADS | MySQL | 6 表 | 应用指标，直接对接 FineBI |

---

## 🔧 集群信息速查

| 主机 | IP | 角色 |
|------|-----|------|
| master-01 | 192.168.10.128 | NameNode + ResourceManager + Spark Driver |
| work-01 | 192.168.10.129 | DataNode + NodeManager |
| work-02 | 192.168.10.130 | DataNode + NodeManager |
| work-03 | 192.168.10.131 | DataNode + NodeManager |
| db-server | 192.168.10.144 | PostgreSQL 15 + MySQL |

| 组件 | 版本 |
|------|------|
| Java | OpenJDK 1.8.0 |
| Hadoop | 3.3.6 |
| Spark | 3.5.8 |
| Hive | 3.1.2 |
| PostgreSQL | 15 |
| MySQL | — |

> 详细环境配置、常见问题排查、面试表述建议请参考：[README_CLUSTER.md](./README_CLUSTER.md)
