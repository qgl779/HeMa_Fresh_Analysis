#!/bin/bash
set -e

PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"

echo "========================================="
echo " 盒马生鲜数据分析平台 - Linux 环境初始化"
echo "========================================="
echo ""

# ---------- 1. 检测 Java ----------
echo "[1/5] 检测 Java 环境..."
if command -v java &>/dev/null; then
    JAVA_VER=$(java -version 2>&1 | head -1 | awk -F '"' '{print $2}')
    echo "  Java 版本: $JAVA_VER"
else
    echo "  [ERROR] Java 未安装，请先安装 OpenJDK 8 或 11"
    echo "  安装命令: sudo apt install openjdk-11-jdk   (Debian/Ubuntu)"
    echo "            sudo yum install java-11-openjdk   (CentOS/RHEL)"
    exit 1
fi

# ---------- 2. 检测 Python ----------
echo "[2/5] 检测 Python 环境..."
if command -v python3 &>/dev/null; then
    PY_VER=$(python3 --version)
    echo "  $PY_VER"
else
    echo "  [ERROR] Python3 未安装"
    echo "  安装命令: sudo apt install python3 python3-pip   (Debian/Ubuntu)"
    echo "            sudo yum install python3 python3-pip   (CentOS/RHEL)"
    exit 1
fi

# ---------- 3. 安装 Python 依赖 ----------
echo "[3/5] 安装 Python 依赖..."
pip3 install -r "$PROJECT_ROOT/config/requirements.txt" -q
echo "  依赖安装完成"

# ---------- 4. 检测 PostgreSQL ----------
echo "[4/5] 检测 PostgreSQL..."
if command -v psql &>/dev/null; then
    PG_VER=$(psql --version | awk '{print $3}')
    echo "  PostgreSQL 版本: $PG_VER"

    # 尝试初始化数据库
    echo "  初始化数据库和表..."
    if psql -U postgres -c "SELECT 1" &>/dev/null 2>&1; then
        psql -U postgres -c "CREATE DATABASE hema_fresh_dw;" 2>/dev/null || echo "  数据库可能已存在，跳过创建"
        psql -U postgres -c "CREATE USER hema_admin WITH PASSWORD 'hema2024';" 2>/dev/null || echo "  用户可能已存在，跳过创建"
        psql -U postgres -c "GRANT ALL PRIVILEGES ON DATABASE hema_fresh_dw TO hema_admin;" 2>/dev/null
        psql -U hema_admin -d hema_fresh_dw -f "$PROJECT_ROOT/sql/01-create-tables.sql"
        echo "  数据库表创建完成"
    else
        echo "  [WARN] 无法连接 PostgreSQL，请手动执行以下命令:"
        echo "    psql -U postgres -c \"CREATE DATABASE hema_fresh_dw;\""
        echo "    psql -U postgres -c \"CREATE USER hema_admin WITH PASSWORD 'hema2024';\""
        echo "    psql -U hema_admin -d hema_fresh_dw -f sql/01-create-tables.sql"
    fi
else
    echo "  [WARN] psql 未安装，跳过数据库初始化"
    echo "  安装命令: sudo apt install postgresql postgresql-client   (Debian/Ubuntu)"
    echo "            sudo yum install postgresql postgresql-client   (CentOS/RHEL)"
fi

# ---------- 5. 下载 Spark (可选) ----------
echo "[5/5] 检测 PySpark..."
if python3 -c "import pyspark" 2>/dev/null; then
    echo "  PySpark 已可用"
else
    echo "  [INFO] 未能导入 PySpark，Spark 脚本将以 spark-submit 方式运行"
    echo "  如需本地模式，请下载 Spark:"
    echo "  wget https://archive.apache.org/dist/spark/spark-3.5.0/spark-3.5.0-bin-hadoop3.tgz"
    echo "  tar -xzf spark-3.5.0-bin-hadoop3.tgz"
    echo "  export SPARK_HOME=\$PWD/spark-3.5.0-bin-hadoop3"
    echo "  export PATH=\$SPARK_HOME/bin:\$PATH"
fi

echo ""
echo "========================================="
echo " 环境初始化完成！"
echo " 下一步: bash run.sh"
echo "========================================="
