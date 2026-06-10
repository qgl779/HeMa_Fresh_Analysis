#!/bin/bash
# =====================================================================
# 盒马生鲜数仓项目 - 一键全流程执行脚本
# 适用：Hadoop 3.3.6 + Spark 3.5.8 + Hive 3.1.2 + PostgreSQL 15 + MySQL
# 架构：PG(ods) -> HDFS -> Hive ODS -> Hive DWD -> Hive DWS -> MySQL ADS
# 执行：cd /opt/project/hema-fresh-analysis && chmod +x cluster_run.sh && bash cluster_run.sh
# =====================================================================

# ---------- 环境变量 ----------
export PROJECT_ROOT="/opt/project/hema-fresh-analysis"
export PYTHONPATH="${PROJECT_ROOT}:${PYTHONPATH}"
export HADOOP_HOME="/opt/module/hadoop-3.3.6"
export SPARK_HOME="/opt/module/spark-3.5.8"
export HIVE_HOME="/opt/module/hive-3.1.2"
export JAVA_HOME="/usr/local/jdk1.8.0_381"
export PATH="${JAVA_HOME}/bin:${HADOOP_HOME}/bin:${HADOOP_HOME}/sbin:${SPARK_HOME}/bin:${HIVE_HOME}/bin:${PATH}"
export HADOOP_CONF_DIR="${HADOOP_HOME}/etc/hadoop"
export YARN_CONF_DIR="${HADOOP_HOME}/etc/hadoop"
export HDFS_BASE_PATH="hdfs://192.168.10.128:9000/hema_fresh"
export PG_HOST="192.168.10.144"
export PG_USER="hema_admin"
export PG_DB="hema_fresh_dw"
export MYSQL_HOST="192.168.10.144"
export MYSQL_USER="hema_ads"
export MYSQL_PWD="hema2024"
export MYSQL_DB="hema_fresh_ads"

cd "${PROJECT_ROOT}" || die "无法切换到项目目录: ${PROJECT_ROOT}"

TOTAL_STEPS=13

# ---------- 颜色输出 ----------
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info()  { echo -e "${GREEN}[INFO]${NC}  $1"; }
log_warn()  { echo -e "${YELLOW}[WARN]${NC}  $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_step()  { echo -e "\n${BLUE}==============================================================${NC}"; echo -e "${BLUE}>>> $1${NC}"; echo -e "${BLUE}==============================================================${NC}"; }

die() {
    log_error "$1"
    exit 1
}

check_rc() {
    local rc=$1
    local msg=$2
    if [ $rc -ne 0 ]; then
        die "${msg}（退出码 $rc）"
    fi
}

# ---------- Spark 公共参数 ----------
SPARK_JARS="/opt/module/spark-3.5.8/jars/postgresql-42.7.1.jar,/opt/module/spark-3.5.8/jars/mysql-connector-java-8.0.28.jar"

SPARK_SUBMIT_OPTS="\
--master yarn \
--deploy-mode client \
--driver-memory 2g \
--num-executors 3 \
--executor-cores 2 \
--executor-memory 4g \
--conf spark.driver.host=192.168.10.128 \
--conf spark.sql.shuffle.partitions=200 \
--conf spark.sql.adaptive.enabled=true"

if [ -f "/opt/module/spark-3.5.8/jars/postgresql-42.7.1.jar" ] && [ -f "/opt/module/spark-3.5.8/jars/mysql-connector-java-8.0.28.jar" ]; then
    SPARK_SUBMIT_OPTS="${SPARK_SUBMIT_OPTS} --jars ${SPARK_JARS}"
    log_info "使用本地 JDBC jars: ${SPARK_JARS}"
else
    log_warn "未找到本地 JDBC jars，将使用 --packages 方式（首次运行较慢）"
    SPARK_SUBMIT_OPTS="${SPARK_SUBMIT_OPTS} --packages org.postgresql:postgresql:42.7.1,mysql:mysql-connector-java:8.0.28"
fi

# Step 4 (00) and Step 5 (01) need PG JDBC
SPARK_OPTS_PG="${SPARK_SUBMIT_OPTS}"
# Step 7+ need only MySQL JDBC — keep all jars anyway for simplicity

submit_spark() {
    local STEP_N=$1
    local SCRIPT=$2
    local DESC=$3

    log_step "[Step ${STEP_N}/${TOTAL_STEPS}] ${DESC}"
    log_info "执行脚本: spark/${SCRIPT}"

    spark-submit ${SPARK_SUBMIT_OPTS} "spark/${SCRIPT}"
    check_rc $? "Spark 作业失败: spark/${SCRIPT}"
    log_info "[Step ${STEP_N}/${TOTAL_STEPS}] ${DESC} 完成"
}

# ============================================================
# Step 1: 环境检查
# ============================================================
log_step "[Step 1/${TOTAL_STEPS}] 环境检查 —— java / hadoop / spark / hive / python3 / pg / mysql"

log_info "[1.1] 检查 java"
if ! command -v java >/dev/null 2>&1; then
    die "java 未安装或不在 PATH 中（当前 JAVA_HOME=${JAVA_HOME}）"
fi
java -version 2>&1 | head -n 1

log_info "[1.2] 检查 hadoop"
if ! command -v hadoop >/dev/null 2>&1; then
    die "hadoop 未安装或不在 PATH 中（当前 HADOOP_HOME=${HADOOP_HOME}）"
fi
hadoop version 2>&1 | head -n 1

log_info "[1.3] 检查 spark"
if ! command -v spark-submit >/dev/null 2>&1; then
    die "spark-submit 未安装或不在 PATH 中（当前 SPARK_HOME=${SPARK_HOME}）"
fi
spark-submit --version 2>&1 | head -n 2

log_info "[1.4] 检查 hive"
if ! command -v hive >/dev/null 2>&1; then
    die "hive 未安装或不在 PATH 中（当前 HIVE_HOME=${HIVE_HOME}）"
fi
log_info "hive 就绪"

log_info "[1.5] 检查 python3"
if ! command -v python3 >/dev/null 2>&1; then
    die "python3 未安装或不在 PATH 中"
fi
python3 --version

log_info "[1.6] 检查 Python 依赖（psycopg2）"
python3 -c "import psycopg2; print('psycopg2', psycopg2.__version__)" 2>/dev/null || die "psycopg2 未安装，请先: pip install psycopg2-binary"

log_info "[1.7] 检查 HDFS 可达性"
if ! hdfs dfs -test -d / 2>/dev/null; then
    die "HDFS 不可达，请检查 NameNode（hdfs://192.168.10.128:9000）"
fi
log_info "HDFS 正常（hdfs://192.168.10.128:9000）"

log_info "[1.8] 检查 YARN ResourceManager"
if yarn application -list 2>&1 | grep -q "Total\|Applications"; then
    log_info "YARN RM 正常"
else
    log_warn "YARN RM 响应异常，可能未完全就绪，继续尝试..."
fi

log_info "[1.9] 检查 PostgreSQL（${PG_HOST}:5432）"
if command -v pg_isready >/dev/null 2>&1; then
    pg_isready -h "${PG_HOST}" -p 5432 -U "${PG_USER}" -d "${PG_DB}" 2>&1 || die "PostgreSQL 不可达"
    log_info "PostgreSQL 正常"
else
    log_warn "pg_isready 未找到，跳过 PostgreSQL 连通性检查"
fi

log_info "[1.10] 检查 MySQL（${MYSQL_HOST}:3306）"
if command -v mysql >/dev/null 2>&1; then
    mysql -h "${MYSQL_HOST}" -u "${MYSQL_USER}" -p"${MYSQL_PWD}" -e "SELECT 1;" >/dev/null 2>&1 || die "MySQL 不可达"
    log_info "MySQL 正常"
else
    log_warn "mysql 客户端未找到，跳过 MySQL 连通性检查"
fi

log_info "[Step 1/${TOTAL_STEPS}] 环境检查完成"

# ============================================================
# Step 2: 生成数据并写入 PG ods + 上传 HDFS
# ============================================================
log_step "[Step 2/${TOTAL_STEPS}] 生成模拟数据 -> 写入 PostgreSQL ods schema -> 上传 HDFS ods_raw"

python3 data/generate_hema_data.py --write-pg --upload-hdfs
check_rc $? "数据生成/写入/上传失败"
log_info "[Step 2/${TOTAL_STEPS}] 完成"

# ============================================================
# Step 3: 执行 Hive DDL 建表
# ============================================================
log_step "[Step 3/${TOTAL_STEPS}] 执行 Hive DDL —— 创建 hema_fresh 库及各层表"

log_info "执行: hive -f sql/02-hive-ddl.sql"
hive -f sql/02-hive-ddl.sql
check_rc $? "Hive DDL 建表失败"
log_info "[Step 3/${TOTAL_STEPS}] 完成"

# ============================================================
# Step 4: 执行 MySQL ADS DDL 建表
# ============================================================
log_step "[Step 4/${TOTAL_STEPS}] 执行 MySQL ADS DDL —— 创建 hema_fresh_ads 库及 BI 表"

if command -v mysql >/dev/null 2>&1; then
    log_info "执行: mysql -h ${MYSQL_HOST} -u ${MYSQL_USER} -p*** < sql/03-mysql-ads-ddl.sql"
    mysql -h "${MYSQL_HOST}" -u "${MYSQL_USER}" -p"${MYSQL_PWD}" < sql/03-mysql-ads-ddl.sql
    check_rc $? "MySQL ADS DDL 建表失败"
    log_info "[Step 4/${TOTAL_STEPS}] 完成"
else
    log_error "mysql 客户端未安装，无法执行 ADS DDL。请手动在 db-server 上执行 sql/03-mysql-ads-ddl.sql"
    die "缺少 mysql 客户端"
fi

# ============================================================
# Step 5: Spark —— PG 抽取到 HDFS/Hive ODS
# ============================================================
submit_spark 5 "00_extract_pg_to_hdfs.py" "PG ods -> HDFS / Hive ods 层抽取"

# ============================================================
# Step 6: Spark —— 数据清洗 ODS -> DWD
# ============================================================
submit_spark 6 "01_data_cleaning.py" "数据清洗 —— ods -> dwd"

# ============================================================
# Step 7: Spark —— 特征聚合 DWD -> DWS + HDFS Features
# ============================================================
submit_spark 7 "02_feature_engineering.py" "特征聚合 —— dwd -> dws + HDFS 中间数据集"

# ============================================================
# Step 8: Spark —— 销量预测 DWS -> MySQL ads_sales_forecast
# ============================================================
submit_spark 8 "03_sales_prediction.py" "销量预测 —— dws -> MySQL ads_sales_forecast"

# ============================================================
# Step 9: Spark —— 库存优化 DWS -> MySQL ads_inventory_optimization
# ============================================================
submit_spark 9 "04_inventory_optimization.py" "库存优化 —— dws -> MySQL ads_inventory_optimization"

# ============================================================
# Step 10: Spark —— 用户画像/行为分析 -> MySQL ads_user_segment_report
# ============================================================
submit_spark 10 "05_user_behavior_analysis.py" "用户画像 / RFM 分层 —— dws -> MySQL ads_user_segment_report"

# ============================================================
# Step 11: Spark —— 品类销售排名 -> MySQL ads_category_ranking
# ============================================================
submit_spark 11 "06_category_ranking.py" "品类销售排名 —— dwd+dim_product -> MySQL ads_category_ranking"

# ============================================================
# Step 12: Spark —— DWS 聚合 -> MySQL ads_daily_sales_summary + ads_membership_contribution
# ============================================================
submit_spark 12 "07_ads_to_mysql.py" "每日销售汇总+会员贡献 —— dws -> MySQL ads_daily_sales_summary + ads_membership_contribution"

# ============================================================
# Step 13: 验证 —— Hive SHOW TABLES + MySQL SHOW TABLES + 行数检查
# ============================================================
log_step "[Step 13/${TOTAL_STEPS}] 结果验证 —— Hive 表列表 + MySQL ads 表列表 + 行数检查"

log_info "[13.1] Hive: USE hema_fresh; SHOW TABLES;"
hive -e "USE hema_fresh; SHOW TABLES;"
check_rc $? "Hive 查询失败"

log_info "[13.2] MySQL: USE hema_fresh_ads; SHOW TABLES; 及行数"
if command -v mysql >/dev/null 2>&1; then
    echo ""
    echo "--- MySQL ADS 表行数 ---"
    for ads_table in ads_sales_forecast ads_inventory_optimization ads_user_segment_report ads_daily_sales_summary ads_category_ranking ads_membership_contribution; do
        row_count=$(mysql -h "${MYSQL_HOST}" -u "${MYSQL_USER}" -p"${MYSQL_PWD}" -N -e "SELECT COUNT(*) FROM ${MYSQL_DB}.${ads_table};" 2>/dev/null)
        if [ -n "$row_count" ]; then
            log_info "  ${ads_table}: ${row_count} 行"
        else
            log_warn "  ${ads_table}: 查询失败"
        fi
    done
    echo ""
else
    log_warn "mysql 客户端未安装，跳过 MySQL 验证"
fi

log_info "[Step 13/${TOTAL_STEPS}] 完成"

# ============================================================
# 结尾：汇总
# ============================================================
echo ""
echo "=================================================================="
echo -e "${GREEN}=== 盒马生鲜数仓全流程执行完成 ===${NC}"
echo "=================================================================="
log_info "各 HDFS 目录检查："
echo ""

for dir_name in ods_raw ods dwd dws features; do
    DIR_PATH="${HDFS_BASE_PATH}/${dir_name}"
    echo "-----------------------------------------------------------"
    log_info "[${dir_name}] ${DIR_PATH}"
    if hdfs dfs -test -d "${DIR_PATH}" 2>/dev/null; then
        local_cnt=$(hdfs dfs -count "${DIR_PATH}" 2>/dev/null | awk '{print $2}')
        log_info "子项数：${local_cnt}"
        hdfs dfs -ls -h "${DIR_PATH}" 2>/dev/null | tail -n 3
    else
        log_warn "目录不存在"
    fi
done

echo ""
echo "=================================================================="
log_info "全流程执行完毕  请检查上方各目录和表行数输出"
echo "=================================================================="
