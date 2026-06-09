import os
import sys
import subprocess
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent
DATA_SCRIPT = PROJECT_ROOT / "data" / "generate_hema_data.py"
SPARK_DIR = PROJECT_ROOT / "spark"


def run_step(step_name, script_path, cwd=None):
    print(f"\n{'='*60}")
    print(f"  [{step_name}] 执行中...")
    print(f"{'='*60}")
    result = subprocess.run(
        [sys.executable, str(script_path)],
        cwd=str(cwd) if cwd else str(PROJECT_ROOT),
        capture_output=False
    )
    if result.returncode != 0:
        print(f"[ERROR] {step_name} 失败 (exit code: {result.returncode})")
        return False
    print(f"[OK] {step_name} 完成")
    return True


if __name__ == "__main__":
    print("=" * 60)
    print("   盒马生鲜数据分析平台 — 一键执行脚本")
    print("=" * 60)

    steps = [
        ("数据生成", DATA_SCRIPT),
        ("PySpark 数据清洗", SPARK_DIR / "01_data_cleaning.py"),
        ("特征工程", SPARK_DIR / "02_feature_engineering.py"),
        ("销量预测模型训练", SPARK_DIR / "03_sales_prediction.py"),
        ("库存优化分析", SPARK_DIR / "04_inventory_optimization.py"),
        ("用户行为分析", SPARK_DIR / "05_user_behavior_analysis.py"),
    ]

    for name, script in steps:
        if not script.exists():
            print(f"[SKIP] {name}: 文件不存在 ({script})")
            continue
        success = run_step(name, script)
        if not success:
            print(f"\n[STOP] {name} 失败，停止后续步骤")
            sys.exit(1)

    print("\n" + "=" * 60)
    print("   全部流程执行完毕！")
    print(f"   结果文件: {PROJECT_ROOT / 'data' / 'features'}")
    print(f"   模型文件: {PROJECT_ROOT / 'models'}")
    print("=" * 60)
