#!/bin/bash
set -e

PROJECT_ROOT="$(cd "$(dirname "$0")" && pwd)"
cd "$PROJECT_ROOT"

export PYTHONPATH="$PROJECT_ROOT:$PYTHONPATH"

GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo "=============================================="
echo " 盒马生鲜数据分析平台 - 全流程执行"
echo "=============================================="
echo ""

# 自动检测运行模式
if [ "$1" == "spark-submit" ]; then
    RUN_MODE="spark-submit"
    echo -e "${YELLOW}[INFO] 运行模式: spark-submit (集群/YARN)${NC}"
elif [ "$1" == "local" ] || [ -z "$1" ]; then
    RUN_MODE="local"
    echo -e "${YELLOW}[INFO] 运行模式: python3 (本地/Local)${NC}"
else
    echo "用法: bash run.sh [local|spark-submit]"
    echo "  local       - 本地模式，使用 python3 直接运行 (默认)"
    echo "  spark-submit - 集群模式，使用 spark-submit 提交"
    exit 1
fi

run_python() {
    local step_name="$1"
    local script="$2"
    echo ""
    echo -e "${GREEN}>>> [${step_name}] ${NC}"
    echo "    脚本: ${script}"
    python3 "$script"
    echo -e "${GREEN}    [OK] ${step_name} 完成${NC}"
}

run_spark_submit() {
    local step_name="$1"
    local script="$2"
    echo ""
    echo -e "${GREEN}>>> [${step_name}] ${NC}"
    echo "    脚本: ${script}"
    spark-submit \
        --master yarn \
        --deploy-mode client \
        --executor-memory 4g \
        --driver-memory 2g \
        --num-executors 4 \
        "$script"
    echo -e "${GREEN}    [OK] ${step_name} 完成${NC}"
}

# ---------- Step 1: 生成数据集 ----------
if [ "$RUN_MODE" == "local" ]; then
    run_python "数据生成" "data/generate_hema_data.py"
else
    run_python "数据生成" "data/generate_hema_data.py"
fi

# ---------- Step 2: PySpark ETL 清洗 ----------
if [ "$RUN_MODE" == "spark-submit" ]; then
    run_spark_submit "数据清洗" "spark/01_data_cleaning.py"
else
    run_python "数据清洗" "spark/01_data_cleaning.py"
fi

# ---------- Step 3: 特征工程 ----------
if [ "$RUN_MODE" == "spark-submit" ]; then
    run_spark_submit "特征工程" "spark/02_feature_engineering.py"
else
    run_python "特征工程" "spark/02_feature_engineering.py"
fi

# ---------- Step 4: 销量预测 ----------
if [ "$RUN_MODE" == "spark-submit" ]; then
    run_spark_submit "销量预测" "spark/03_sales_prediction.py"
else
    run_python "销量预测" "spark/03_sales_prediction.py"
fi

# ---------- Step 5: 库存优化 ----------
if [ "$RUN_MODE" == "spark-submit" ]; then
    run_spark_submit "库存优化" "spark/04_inventory_optimization.py"
else
    run_python "库存优化" "spark/04_inventory_optimization.py"
fi

# ---------- Step 6: 用户行为分析 ----------
if [ "$RUN_MODE" == "spark-submit" ]; then
    run_spark_submit "用户行为分析" "spark/05_user_behavior_analysis.py"
else
    run_python "用户行为分析" "spark/05_user_behavior_analysis.py"
fi

echo ""
echo "=============================================="
echo -e "${GREEN} 全部流程执行完毕！${NC}"
echo "  输出数据: ${PROJECT_ROOT}/data/features/"
echo "  模型文件: ${PROJECT_ROOT}/models/"
echo "=============================================="
