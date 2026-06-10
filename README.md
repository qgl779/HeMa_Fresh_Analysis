# 盒马生鲜数据分析平台

> **Hadoop HDFS + PySpark + PostgreSQL + FineBI** 全栈生鲜零售分析平台  
> 三大核心：**销量预测 · 库存优化 · 用户行为分析**

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

## 🐧 Linux 部署与执行（推荐）

### 一键流程

```bash
# 1. 克隆/上传项目到 Linux 服务器
scp -r hema-fresh-analysis user@your-server:/opt/

# 2. SSH 登录后，给脚本执行权限
cd /opt/hema-fresh-analysis
chmod +x setup.sh run.sh

# 3. 初始化环境（安装依赖 + 建数据库表）
bash setup.sh

# 4. 运行全流程
bash run.sh            # 本地模式 (python3)
bash run.sh spark-submit   # 集群模式 (spark-submit)
```

### 前置依赖清单

执行 `setup.sh` 前请确保以下组件已安装：

| 组件 | 最低版本 | 验证命令 | 安装命令 (Ubuntu) |
|------|---------|---------|-------------------|
| Java | 8 | `java -version` | `sudo apt install openjdk-11-jdk` |
| Python3 | 3.9 | `python3 --version` | `sudo apt install python3 python3-pip` |
| PostgreSQL | 13 | `psql --version` | `sudo apt install postgresql postgresql-client` |
| Spark (可选) | 3.5 | `spark-submit --version` | 下载解压到 `/opt/spark` |

### 手动分步执行

如果不想用 `bash run.sh`，也可以逐脚本运行：

```bash
cd /opt/hema-fresh-analysis
export PYTHONPATH=$(pwd):$PYTHONPATH

# Step 1: 生成数据集 (纯 Python，无需 Spark)
python3 data/generate_hema_data.py

# Step 2: PySpark ETL 数据清洗
python3 spark/01_data_cleaning.py
# 或集群:
spark-submit --master yarn --executor-memory 4g --num-executors 4 spark/01_data_cleaning.py

# Step 3: 特征工程
python3 spark/02_feature_engineering.py

# Step 4: 销量预测 (训练 RandomForest + GBT 模型)
python3 spark/03_sales_prediction.py

# Step 5: 库存优化 (安全库存 / EOQ / 预警)
python3 spark/04_inventory_optimization.py

# Step 6: 用户行为分析 (RFM / 漏斗 / 偏好 / 会员)
python3 spark/05_user_behavior_analysis.py
```

### PostgreSQL 手动初始化

如果 `setup.sh` 的数据库步骤失败，手动执行：

```bash
sudo -u postgres psql
```

```sql
CREATE DATABASE hema_fresh_dw;
CREATE USER hema_admin WITH PASSWORD 'hema2024';
GRANT ALL PRIVILEGES ON DATABASE hema_fresh_dw TO hema_admin;
\q
```

```bash
psql -U hema_admin -d hema_fresh_dw -f sql/01-create-tables.sql
```

### Hadoop 集群模式

如果你的 Linux 上部署了 Hadoop + YARN 集群：

```bash
# 1. 先把生成的 CSV 上传到 HDFS
hdfs dfs -mkdir -p /hema_fresh/raw
hdfs dfs -put data/raw/*.csv /hema_fresh/raw/

# 2. 修改 config/settings.py 中的数据读取路径
#    (如果用 HDFS，需要在 PySpark 中配置 hdfs:// 路径)

# 3. 以 YARN 模式提交
bash run.sh spark-submit
```

### 修改配置

编辑 `config/settings.py` 适配你的 Linux 环境：

```python
DB_CONFIG = {
    "host": "127.0.0.1",       # PostgreSQL 地址
    "port": 5432,
    "user": "hema_admin",
    "password": "hema2024",    # 改成你的密码
    "database": "hema_fresh_dw"
}

SPARK_CONFIG = {
    "master": "local[*]",       # 集群改成 "yarn"
    "spark.executor.memory": "4g",
    "spark.driver.memory": "2g"
}

HDFS_BASE_PATH = "hdfs://namenode:9000/hema_fresh"  # 改成你的 NameNode
```

---

## 💻 Windows 执行

```powershell
cd hema-fresh-analysis
pip install -r config/requirements.txt

python data/generate_hema_data.py
python spark/01_data_cleaning.py
python spark/02_feature_engineering.py
python spark/03_sales_prediction.py
python spark/04_inventory_optimization.py
python spark/05_user_behavior_analysis.py

# 或一键
python run_all.py
```

---

## 📁 目录结构

```
hema-fresh-analysis/
├── setup.sh                        # ★ Linux 一键环境初始化
├── run.sh                          # ★ Linux 全流程执行脚本
├── run_all.py                      # Windows 全流程执行脚本
├── config/
│   ├── settings.py                 # 全局配置 (DB/Spark/HDFS/品类/门店)
│   └── requirements.txt            # Python 依赖
├── data/
│   └── generate_hema_data.py       # 数据集生成 (6张CSV)
├── spark/
│   ├── 01_data_cleaning.py         # PySpark ETL 清洗
│   ├── 02_feature_engineering.py   # 特征工程
│   ├── 03_sales_prediction.py      # 销量预测 (RF + GBT)
│   ├── 04_inventory_optimization.py # 库存优化 (安全库存/EOQ)
│   └── 05_user_behavior_analysis.py # 用户行为 (RFM/漏斗/偏好)
├── sql/
│   ├── 01-create-tables.sql        # DDL (ODS→DWD→DWS→ADS 四层)
│   └── 02-analysis-queries.sql     # 分析 SQL + FineBI 视图
└── README.md
```

---

## 📊 三大分析能力

### 1. 销量预测
- RandomForest (100 树) + GBT (100 迭代) 双模型对比
- 时序滞后 + 滚动统计 + 时间特征
- 评估：RMSE / MAE / R² / MAPE
- 输出：未来 7 天逐 SKU 预测

### 2. 库存优化
- 安全库存公式：`σ × z × √L`（服务水平 1.65）
- 经济订货批量 EOQ：`√(2DS/H)`
- 四级预警：🔴严重缺货 / 🟡预警 / 🟠超量 / 🟢正常
- 生鲜损耗率趋势分析

### 3. 用户行为
- RFM 五层用户分层（高价值/活跃/沉睡/新/流失）
- 转化漏斗（浏览→点击→详情→加购→收藏→分享）
- 品类偏好 + 复购分析
- 会员价值对比（普通/黄金/钻石/X会员）

---

## 🔧 FineBI 对接

### 数据源配置
```
类型: PostgreSQL
主机: 127.0.0.1
端口: 5432
数据库: hema_fresh_dw
用户: hema_admin
密码: hema2024
```

### 预建视图（直接拖拽到 FineBI）
| 视图 | 用途 |
|------|------|
| `ads.v_inventory_alert_monitor` | 实时库存预警看板 |
| `ads.v_user_rfm_segments` | 用户 RFM 分层画像 |
| `ads.v_waste_analysis` | 生鲜损耗率分析 |
| `ads.v_promotion_effect` | 促销效果对比 |

---

## 📝 数据库分层

| 层 | Schema | 数量 | 职责 |
|----|--------|------|------|
| ODS | `ods` | 3 表 | 贴源层，原始数据 |
| DWD | `dwd` | 3 维度 + 3 事实 | 清洗标准化，类型转换 |
| DWS | `dws` | 4 汇总表 | 日度/月度轻度聚合 |
| ADS | `ads` | 6 表 + 5 视图 | 应用指标，直接对接 FineBI |
