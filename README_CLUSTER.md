# 盒马生鲜数仓项目 - 集群版部署与运行指南

> 适用环境：真实物理/虚拟机集群（Hadoop 3.3.5 + Spark 3.5.1 + PostgreSQL 15）
> 项目根目录：`/root/hema-fresh-analysis`

---

## a) 集群节点信息表

| 主机名       | IP 地址         | 角色                        | 操作系统       |
| ------------ | --------------- | --------------------------- | -------------- |
| `master-01`  | 192.168.10.128  | NameNode + ResourceManager + Spark Driver (YARN client) | CentOS 7 / Ubuntu 20.04 |
| `work-01`    | 192.168.10.129  | DataNode + NodeManager      | CentOS 7       |
| `work-02`    | 192.168.10.130  | DataNode + NodeManager      | CentOS 7       |
| `work-03`    | 192.168.10.131  | DataNode + NodeManager      | CentOS 7       |
| `db-server`  | 192.168.10.144  | PostgreSQL 15 (hema_fresh_dw) | CentOS 7     |

- HDFS 根路径：`hdfs://192.168.10.128:9000/hema_fresh`
- YARN Web UI：`http://192.168.10.128:8088`
- NameNode Web UI：`http://192.168.10.128:9870`
- History Server：`http://192.168.10.128:19888`（如已配置）
- PostgreSQL：`192.168.10.144:5432`  user=`hema_admin`  db=`hema_fresh_dw`

---

## b) 软件版本矩阵

| 组件           | 版本                 | 安装路径 / 说明                                      |
| -------------- | -------------------- | ---------------------------------------------------- |
| Java           | OpenJDK 1.8.0        | `/usr/lib/jvm/java-1.8.0-openjdk`（Spark 3.5 支持） |
| Hadoop         | **3.3.5**            | `/opt/hadoop-3.3.5`（HDFS + YARN）                   |
| Spark          | **3.5.1**            | `/opt/spark-3.5.1`（使用 YARN 作为 cluster manager） |
| PostgreSQL     | **15.x**             | `/var/lib/pgsql/15/data`（192.168.10.144）            |
| Python         | **3.6.8**（建议升级到 3.8+） | `/usr/bin/python3`（Spark 3.5 推荐 Python 3.8+） |
| PySpark        | 3.5.1                | `pip install pyspark==3.5.1`                         |
| psycopg2       | latest               | `pip install psycopg2-binary`                        |
| pandas / numpy | latest（可选）       | 用于辅助分析                                          |

> 环境变量说明（脚本已自动设置）：
> `JAVA_HOME`、`HADOOP_HOME`、`SPARK_HOME`、`HADOOP_CONF_DIR`、`YARN_CONF_DIR`、`PYTHONPATH`

---

## c) 启动前检查清单（共 8 条）

|  #  | 检查项                                                     | 命令 / 方式                                                          | 预期输出                              |
| --- | ---------------------------------------------------------- | --------------------------------------------------------------------- | ------------------------------------- |
|  1  | Java 可用                                                  | `java -version`                                                      | openjdk version "1.8.0_xxx"           |
|  2  | Hadoop 命令                                                 | `hdfs version` / `yarn version`                                       | Hadoop 3.3.5                          |
|  3  | Spark 命令                                                  | `spark-submit --version`                                              | version 3.5.1                         |
|  4  | HDFS 可达                                                   | `hdfs dfs -ls /`                                                      | 无报错，根目录列表正常                |
|  5  | Java 进程（jps）                                           | `jps`                                                                 | NameNode / DataNode / ResourceManager / NodeManager |
|  6  | PostgreSQL 可达                                            | `pg_isready -h 192.168.10.144 -p 5432 -U hema_admin -d hema_fresh_dw` | `accepting connections`               |
|  7  | Python 模块可用                                            | `python3 -c "import pyspark, psycopg2; print('ok')"`                   | 无 ImportError                        |
|  8  | 路径与权限                                                  | `ls -la /root/hema-fresh-analysis`                                     | cluster_run.sh 与 spark/ 目录存在    |

---

## d) 初始化步骤（首次执行）

在 `db-server` 或任意可访问 PostgreSQL 的节点执行：

```bash
# 1. 创建数据库与用户（首次）
psql -h 192.168.10.144 -U postgres <<EOF
CREATE USER hema_admin WITH PASSWORD 'hema2024';
CREATE DATABASE hema_fresh_dw OWNER hema_admin;
\q
EOF

# 2. 初始化 Schema 与表结构
cd /root/hema-fresh-analysis
psql -h 192.168.10.144 -U hema_admin -d hema_fresh_dw -f sql/01-create-tables.sql

# 3. 验证连接
python3 pg_connect_test.py
```

执行完成后应看到：
- 4 个 schema：`ods` / `dwd` / `dws` / `ads`
- 数张基础表：`ods_order_info`、`ods_inventory_snapshot`、`ods_user_behavior` 等

---

## e) 一键全流程命令

**在 `master-01`（192.168.10.128）上执行：**

```bash
cd /root/hema-fresh-analysis
chmod +x cluster_run.sh
bash cluster_run.sh
```

脚本按序完成以下步骤：
1. 集群服务健康检查（HDFS / YARN / PG）
2. 生成模拟数据并上传至 `hdfs://.../hema_fresh/raw`
3. 检查 raw 目录文件数
4. Spark 数据清洗（`01_data_cleaning.py`）
5. Spark 特征工程（`02_feature_engineering.py`）
6. Spark 销售预测（`03_sales_prediction.py`）
7. Spark 库存优化（`04_inventory_optimization.py`）
8. Spark 用户行为分析（`05_user_behavior_analysis.py`）
9. 脚本尾部汇总各 HDFS 输出目录的检查结果

---

## f) 分步手动命令（调试用）

如需要单独运行某一环节，可使用如下命令（在 `/root/hema-fresh-analysis` 目录下）：

```bash
# ===== Step 0: 环境变量 =====
export HADOOP_HOME=/opt/hadoop-3.3.5
export SPARK_HOME=/opt/spark-3.5.1
export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk
export PATH=$JAVA_HOME/bin:$HADOOP_HOME/bin:$SPARK_HOME/bin:$PATH

# ===== Step 1: 生成并上传数据 =====
python3 data/generate_hema_data.py --upload-hdfs

# ===== Step 2: 检查 raw =====
hdfs dfs -ls hdfs://192.168.10.128:9000/hema_fresh/raw

# ===== Step 3: 数据清洗 =====
spark-submit --master yarn --deploy-mode client \
    --executor-memory 4g --driver-memory 2g \
    --num-executors 3 --executor-cores 2 \
    --conf spark.driver.host=192.168.10.128 \
    spark/01_data_cleaning.py

# ===== Step 4: 特征工程 =====
spark-submit --master yarn --deploy-mode client \
    --executor-memory 4g --driver-memory 2g \
    --num-executors 3 --executor-cores 2 \
    --conf spark.driver.host=192.168.10.128 \
    spark/02_feature_engineering.py

# ===== Step 5: 销售预测 =====
spark-submit --master yarn --deploy-mode client \
    --executor-memory 4g --driver-memory 2g \
    --num-executors 3 --executor-cores 2 \
    --conf spark.driver.host=192.168.10.128 \
    spark/03_sales_prediction.py

# ===== Step 6: 库存优化 =====
spark-submit --master yarn --deploy-mode client \
    --executor-memory 4g --driver-memory 2g \
    --num-executors 3 --executor-cores 2 \
    --conf spark.driver.host=192.168.10.128 \
    spark/04_inventory_optimization.py

# ===== Step 7: 用户行为分析 =====
spark-submit --master yarn --deploy-mode client \
    --executor-memory 4g --driver-memory 2g \
    --num-executors 3 --executor-cores 2 \
    --conf spark.driver.host=192.168.10.128 \
    spark/05_user_behavior_analysis.py
```

---

## g) 验证输出

### G.1 HDFS 各目录行数检查

```bash
# raw 目录（原始 CSV）
hdfs dfs -ls -h hdfs://192.168.10.128:9000/hema_fresh/raw
# 统计行数（合并所有文件，可过滤具体文件名）
hdfs dfs -cat hdfs://192.168.10.128:9000/hema_fresh/raw/order_info_*.csv | wc -l
hdfs dfs -cat hdfs://192.168.10.128:9000/hema_fresh/raw/inventory_*.csv | wc -l
hdfs dfs -cat hdfs://192.168.10.128:9000/hema_fresh/raw/user_behavior_*.csv | wc -l

# ods / dwd / dws / ads 目录（parquet / csv）
hdfs dfs -ls -h hdfs://192.168.10.128:9000/hema_fresh/ods
hdfs dfs -ls -h hdfs://192.168.10.128:9000/hema_fresh/dwd
hdfs dfs -ls -h hdfs://192.168.10.128:9000/hema_fresh/dws
hdfs dfs -ls -h hdfs://192.168.10.128:9000/hema_fresh/ads
```

### G.2 PostgreSQL 各表行数检查

```bash
# 方式 1: pg_connect_test.py（已自动输出 schema 表数 + 行数估算）
python3 pg_connect_test.py

# 方式 2: 手动 SQL
psql -h 192.168.10.144 -U hema_admin -d hema_fresh_dw -c "
SELECT
  schemaname,
  tablename,
  schemaname || '.' || tablename AS full_name,
  (xpath('/row/cnt/text()',
     query_to_xml('SELECT COUNT(*) AS cnt FROM ' || quote_ident(schemaname) || '.' || quote_ident(tablename),
                  false, true, '')))[1]::text::bigint AS cnt
FROM pg_tables
WHERE schemaname IN ('ods','dwd','dws','ads')
ORDER BY schemaname, cnt DESC;
"

# 或直接查询特定表
psql -h 192.168.10.144 -U hema_admin -d hema_fresh_dw \
     -c "SELECT COUNT(*) FROM ods.ods_order_info;"
```

---

## h) 常见问题与修复

| #  | 问题 / 报错现象                                               | 根因推测                                                 | 修复建议                                                                 |
| -- | ------------------------------------------------------------- | -------------------------------------------------------- | ------------------------------------------------------------------------ |
| H1 | **YARN 资源不足**：`Container is running beyond memory limits` / `Application Failed` | 每个 Executor 申请的内存 / vcore 超出集群可用容量 | 1. 调小 `--num-executors`、`--executor-memory`、`--executor-cores` <br> 2. 检查 `yarn-site.xml`：`yarn.nodemanager.resource.memory-mb`、`yarn.nodemanager.resource.cpu-vcores` <br> 3. 检查 `yarn.scheduler.maximum-allocation-mb` |
| H2 | **PG 连接被拒绝**：`connection refused` / `no route to host` | PostgreSQL 未启动 / 防火墙阻挡 / `pg_hba.conf` 未允许客户端 IP | 1. `systemctl status postgresql-15` <br> 2. `firewall-cmd --add-port=5432/tcp --permanent && firewall-cmd --reload` <br> 3. 在 `pg_hba.conf` 加入 `host all all 192.168.10.0/24 md5` 并 `SELECT pg_reload_conf();` |
| H3 | **Python 模块缺失**：`ModuleNotFoundError: No module named 'pyspark'` | 环境中未安装或 `PYTHONPATH` 未包含项目根目录 | `pip install pyspark==3.5.1 psycopg2-binary`；执行脚本前确认 `export PYTHONPATH=/root/hema-fresh-analysis:$PYTHONPATH` |
| H4 | **spark-submit 找不到**：`command not found`                 | `SPARK_HOME/bin` 未在 PATH 中                            | `export SPARK_HOME=/opt/spark-3.5.1; export PATH=$SPARK_HOME/bin:$PATH`；或直接 `$SPARK_HOME/bin/spark-submit` |
| H5 | **Executors 起不来**：YARN UI 中 Running=0、一直 `ACCEPTED`  | 资源不足 / NodeManager 失联 / `spark.driver.host` 写成本机内网 IP | 1. 确认脚本参数 `--conf spark.driver.host=192.168.10.128`（必须是 RM 可反连的 IP） <br> 2. `yarn node -list` 检查节点是否为 RUNNING <br> 3. 调小 `--num-executors` 再试 |
| H6 | **Python 3.6 与 Spark 3.5 的兼容警告**                      | Spark 3.5.1 官方推荐 Python 3.8+                         | 推荐升级：`sudo yum install python38 -y`（CentOS 7 需启用 IUS/SCL 源），并将 `PYSPARK_PYTHON` 指向新的 Python：`export PYSPARK_PYTHON=/usr/bin/python3.8`。若必须保留 3.6，可降级 Spark 到 3.2.x。 |
| H7 | **HDFS 写权限报错**：`Permission denied: user=xxx, access=WRITE` | 运行用户非 HDFS 管理员                                   | 在 NameNode 执行 `hdfs dfs -chmod -R 777 /hema_fresh` 或 `hdfs dfs -chown -R root /hema_fresh`（开发环境） |
| H8 | **Executor 日志乱码**                                        | 中文编码未统一                                           | 在启动脚本前 `export LANG=en_US.UTF-8; export LC_ALL=en_US.UTF-8` |

---

## i) 面试表述建议

> 以下话术可用于简历与面试场景，强调项目闭环、技术栈、产出指标。

**项目名称**：盒马生鲜数仓与智能分析平台（Hema Fresh Data Warehouse & Analytics）

**一句话概述**：基于 Hadoop + Spark + PostgreSQL 构建的离线数仓平台，覆盖订单、库存、用户行为三大数据源，完成 ODS→DWD→DWS→ADS 的分层数据治理，并产出销售预测、库存优化、用户画像三类业务指标。

**技术栈（按链路）**：
- 采集层：业务系统 CSV 模拟 → 上传 `HDFS (raw)`
- 存储层：Hadoop 3.3.5（HDFS 3 节点存储，YARN 3 节点调度）
- 计算层：Spark 3.5.1 on YARN（PySpark DataFrame + Spark SQL）
- 下游层：PostgreSQL 15（ODS/DWD/DWS/ADS 四层 Schema + BI 查询）
- 编排层：Bash 脚本 `cluster_run.sh` 做一键编排与退出码校验
- 监控层：YARN UI / HDFS Web UI / PG `pg_connect_test.py` 数据完整性校验

**关键产出**：
1. ODS 层：10+ 张原始贴源表，HDFS raw 目录文件数 ≥ 20，行数 100w 级
2. DWD 层：完成数据清洗（空值、去重、字段规范、类型转换），输出明细宽表
3. DWS 层：特征工程产出 20+ 业务指标（GMV、客单价、SKU 动销、渠道转化）
4. ADS 层：
   - 销售预测（按门店/品类/日期聚合，含同比环比）
   - 库存优化（安全库存、补货点、滞销 SKU 识别）
   - 用户行为分析（会话数、停留时长、漏斗转化、高频商品）

**业务价值**：支撑运营每日复盘、采购智能补货、会员精细化运营。

**规模与性能**：
- 3 台 DataNode / 3 个 Executor / 每 Executor 4 GB + 2 core
- 单跑全流程约 8~15 分钟（随数据规模浮动）
- 数据落库到 PostgreSQL，支持秒级 ad-hoc 查询

---

**文件位置速查**
- 一键脚本：`/root/hema-fresh-analysis/cluster_run.sh`
- PG 连接校验：`/root/hema-fresh-analysis/pg_connect_test.py`
- 建表脚本：`/root/hema-fresh-analysis/sql/01-create-tables.sql`
- Spark 作业目录：`/root/hema-fresh-analysis/spark/`
