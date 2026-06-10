import os
import sys
import csv
import random
import uuid
import argparse
import subprocess
from datetime import datetime, timedelta
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from config.settings import (
    DB_CONFIG, CITIES, STORES, CATEGORIES, CUSTOMER_TAGS,
    WEEKDAY_EFFECT, SEASON_EFFECT, MEMBERSHIP_LEVELS,
    PRODUCT_BASE_PRICES, SHELF_LIFE_DAYS
)

random.seed(2024)

DATA_DIR = Path(__file__).resolve().parent / "raw"
DATA_DIR.mkdir(parents=True, exist_ok=True)

NUM_USERS = 10000
NUM_PRODUCTS = len([p for plist in CATEGORIES.values() for p in plist])
NUM_ORDERS = 500000
START_DATE = datetime(2024, 1, 1)
END_DATE = datetime(2024, 12, 31)
DATE_RANGE = (END_DATE - START_DATE).days

PRODUCT_LIST = []
for category, products in CATEGORIES.items():
    for product in products:
        PRODUCT_LIST.append({
            "product_name": product,
            "category": category,
            "base_price": PRODUCT_BASE_PRICES[product],
            "shelf_life": SHELF_LIFE_DAYS[product]
        })

PG_ODS_SCHEMA = "ods"
PG_ODS_TABLES = [
    "dim_product",
    "dim_store",
    "dim_user",
    "ods_order_info",
    "ods_inventory_snapshot",
    "ods_user_behavior",
]


def upload_to_hdfs(local_dir, hdfs_dir):
    local_path = Path(local_dir)
    if not local_path.is_dir():
        print(f"[HDFS][WARN] 本地目录不存在，跳过上传: {local_dir}")
        return

    csv_files = sorted(local_path.glob("*.csv"))
    if not csv_files:
        print(f"[HDFS][WARN] 本地目录无 CSV 文件，跳过上传: {local_dir}")
        return

    print(f"[HDFS] 开始上传 {len(csv_files)} 个 CSV 文件 -> {hdfs_dir}")

    try:
        mkdir_res = subprocess.run(
            ["hdfs", "dfs", "-mkdir", "-p", hdfs_dir],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True
        )
        if mkdir_res.returncode != 0:
            print(f"[HDFS][ERROR] mkdir -p 失败: {mkdir_res.stderr.strip()}")
            return
        print(f"[HDFS][OK] 目录就绪: {hdfs_dir}")
    except FileNotFoundError:
        print("[HDFS][ERROR] 未找到 hdfs 命令，请确认 Hadoop 客户端已安装且在 PATH 中")
        return
    except Exception as e:
        print(f"[HDFS][ERROR] mkdir 异常: {e}")
        return

    success_count = 0
    for csv_file in csv_files:
        try:
            put_res = subprocess.run(
                ["hdfs", "dfs", "-put", "-f", str(csv_file), hdfs_dir.rstrip("/") + "/"],
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                universal_newlines=True
            )
            if put_res.returncode != 0:
                print(f"[HDFS][FAIL] {csv_file.name} 上传失败: {put_res.stderr.strip()}")
            else:
                print(f"[HDFS][OK] 上传成功: {csv_file.name}")
                success_count += 1
        except Exception as e:
            print(f"[HDFS][FAIL] {csv_file.name} 上传异常: {e}")

    print(f"[HDFS] 上传完成: 成功 {success_count}/{len(csv_files)}")


def _get_pg_conn():
    try:
        import psycopg2
    except ImportError:
        print("[PG][ERROR] 未安装 psycopg2，请先执行: pip install psycopg2-binary")
        return None
    try:
        conn = psycopg2.connect(
            host=DB_CONFIG["host"],
            port=DB_CONFIG["port"],
            user=DB_CONFIG["user"],
            password=DB_CONFIG["password"],
            dbname=DB_CONFIG["database"],
        )
        conn.autocommit = False
        return conn
    except Exception as e:
        print(f"[PG][ERROR] 连接失败: {e}")
        return None


def _pg_write_table(conn, schema, table, columns, rows, batch_size=5000):
    import psycopg2
    full_name = f'"{schema}"."{table}"'
    col_str = ", ".join(f'"{c}"' for c in columns)
    placeholders = ", ".join(["%s"] * len(columns))
    sql_truncate = f"TRUNCATE TABLE {full_name}"
    sql_insert = f"INSERT INTO {full_name} ({col_str}) VALUES ({placeholders})"

    try:
        cur = conn.cursor()
        cur.execute(sql_truncate)
        print(f"[PG] 已清空表 {full_name}")
        total = 0
        batch = []
        for row in rows:
            batch.append(row)
            if len(batch) >= batch_size:
                cur.executemany(sql_insert, batch)
                total += len(batch)
                batch = []
        if batch:
            cur.executemany(sql_insert, batch)
            total += len(batch)
        conn.commit()
        cur.close()
        print(f"[PG] {full_name} 写入完成: {total} 行")
        return total
    except Exception as e:
        conn.rollback()
        print(f"[PG][ERROR] 写入 {full_name} 失败: {e}")
        raise


def generate_products(write_pg=False, conn=None):
    filepath = DATA_DIR / "dim_product.csv"
    rows = []
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["product_id", "product_name", "category", "base_price",
                          "shelf_life_days", "supplier", "origin", "storage_type", "unit"])
        for idx, p in enumerate(PRODUCT_LIST):
            pid = f"P{idx+1:05d}"
            supplier = f"供应商_{random.choice('ABCDEFGH')}"
            origins = ["山东", "云南", "海南", "新疆", "进口", "浙江", "广东", "福建", "四川", "江苏"]
            storage_types = ["常温", "冷藏", "冷冻"]
            if p["category"] in ("蔬菜", "水果", "肉禽蛋", "海鲜水产", "熟食"):
                st = random.choice(["冷藏", "冷冻"])
            elif p["category"] in ("冷冻食品", "乳制品"):
                st = random.choice(["冷藏", "冷冻"])
            else:
                st = "常温"
            units = ["500g", "1kg", "250g", "300g", "盒", "袋", "瓶", "个", "份"]
            row = [pid, p["product_name"], p["category"], p["base_price"],
                   p["shelf_life"], supplier, random.choice(origins),
                   st, random.choice(units)]
            writer.writerow(row)
            rows.append(row)
    print(f"[OK] dim_product.csv — {len(PRODUCT_LIST)} products")

    if write_pg and conn is not None:
        cols = ["product_id", "product_name", "category", "base_price",
                "shelf_life_days", "supplier", "origin", "storage_type", "unit"]
        _pg_write_table(conn, PG_ODS_SCHEMA, "dim_product", cols, rows)


def generate_stores(write_pg=False, conn=None):
    filepath = DATA_DIR / "dim_store.csv"
    rows = []
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["store_id", "store_name", "city", "district", "area_sqm", "opening_date"])
        for idx, store in enumerate(STORES):
            sid = f"S{idx+1:03d}"
            city = "上海" if any(x in store for x in ["金桥", "长宁", "大宁", "虹桥", "静安"]) else \
                   "北京" if any(x in store for x in ["十里堡", "朝阳", "海淀", "亦庄"]) else \
                   "深圳" if any(x in store for x in ["南山", "福田", "龙华"]) else \
                   "广州" if any(x in store for x in ["天河", "海珠", "番禺"]) else \
                   "杭州" if any(x in store for x in ["西湖", "滨江", "城西"]) else \
                   "成都" if any(x in store for x in ["锦江", "武侯"]) else \
                   "武汉" if any(x in store for x in ["武昌", "光谷"]) else \
                   "南京" if any(x in store for x in ["新街口", "鼓楼"]) else \
                   "苏州" if any(x in store for x in ["园区", "昆山"]) else "西安"
            district = store.replace("店", "")
            area = random.randint(3000, 12000)
            open_date = (datetime(2020, 1, 1) + timedelta(days=random.randint(0, 1400))).strftime("%Y-%m-%d")
            row = [sid, f"盒马鲜生·{store}", city, district, area, open_date]
            writer.writerow(row)
            rows.append(row)
    print(f"[OK] dim_store.csv — {len(STORES)} stores")

    if write_pg and conn is not None:
        cols = ["store_id", "store_name", "city", "district", "area_sqm", "opening_date"]
        _pg_write_table(conn, PG_ODS_SCHEMA, "dim_store", cols, rows)


def generate_users(write_pg=False, conn=None):
    filepath = DATA_DIR / "dim_user.csv"
    rows = []
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["user_id", "user_name", "gender", "age", "city",
                          "membership", "register_date", "user_tag", "lifetime_value"])
        for i in range(1, NUM_USERS + 1):
            uid = f"U{i:06d}"
            gender = random.choice(["男", "女"])
            age = random.randint(18, 65)
            city = random.choice(CITIES)
            membership = random.choices(
                MEMBERSHIP_LEVELS,
                weights=[0.45, 0.30, 0.18, 0.07],
                k=1
            )[0]
            reg_date = (START_DATE - timedelta(days=random.randint(30, 730))).strftime("%Y-%m-%d")
            user_tag = random.choice(CUSTOMER_TAGS)
            ltv = round(random.uniform(200, 50000), 2)
            row = [uid, f"用户_{uid}", gender, age, city, membership, reg_date, user_tag, ltv]
            writer.writerow(row)
            rows.append(row)
    print(f"[OK] dim_user.csv — {NUM_USERS} users")

    if write_pg and conn is not None:
        cols = ["user_id", "user_name", "gender", "age", "city",
                "membership", "register_date", "user_tag", "lifetime_value"]
        _pg_write_table(conn, PG_ODS_SCHEMA, "dim_user", cols, rows)


def generate_orders(write_pg=False, conn=None):
    filepath = DATA_DIR / "ods_order_info.csv"
    statuses = ["completed", "completed", "completed", "completed",
                "completed", "refunded", "cancelled", "completed", "completed", "returned"]
    channels = ["线上-APP", "线上-小程序", "线下-门店"]
    channel_weights = [0.45, 0.25, 0.30]

    rows = []
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["order_id", "user_id", "store_id", "product_id",
                          "order_date", "order_hour", "quantity", "unit_price",
                          "total_amount", "discount_amount", "pay_amount",
                          "channel", "status", "delivery_type", "delivery_duration_min"])

        for i in range(1, NUM_ORDERS + 1):
            oid = f"ORD{datetime.now().strftime('%Y%m%d')}{i:08d}"
            uid = f"U{random.randint(1, NUM_USERS):06d}"
            sid = f"S{random.randint(1, len(STORES)):03d}"
            prod = random.choice(PRODUCT_LIST)
            pid = f"P{PRODUCT_LIST.index(prod)+1:05d}"
            order_date = START_DATE + timedelta(days=random.randint(0, DATE_RANGE))
            dow = order_date.weekday()
            hour_choices = list(range(8, 22))
            weights = [1, 1, 1, 3, 5, 6, 6, 4, 3, 4, 5, 7, 5, 4]
            order_hour = random.choices(hour_choices, weights=weights, k=1)[0]

            quantity = max(1, int(abs(random.gauss(2, 1.5))))
            month = order_date.month
            seasonal_factor = SEASON_EFFECT.get(month, 1.0)
            weekday_factor = WEEKDAY_EFFECT.get(dow, 1.0)
            price_multiplier = seasonal_factor * weekday_factor * random.uniform(0.85, 1.15)
            unit_price = round(prod["base_price"] * price_multiplier, 2)
            total_amount = round(unit_price * quantity, 2)
            discount_rate = random.choice([0, 0, 0, 0.05, 0.10, 0.15, 0.20, 0.30, 0.50])
            discount_amount = round(total_amount * discount_rate, 2)
            pay_amount = round(total_amount - discount_amount, 2)

            channel = random.choices(channels, weights=channel_weights, k=1)[0]
            status = random.choice(statuses)

            if channel in ("线上-APP", "线上-小程序"):
                delivery_type = random.choice(["30分钟达", "半日达", "次日达"])
                if delivery_type == "30分钟达":
                    delivery_duration = random.randint(15, 45)
                elif delivery_type == "半日达":
                    delivery_duration = random.randint(120, 300)
                else:
                    delivery_duration = random.randint(600, 1500)
            else:
                delivery_type = "自提"
                delivery_duration = 0

            row = [oid, uid, sid, pid, order_date.strftime("%Y-%m-%d"),
                   order_hour, quantity, unit_price, total_amount,
                   discount_amount, pay_amount, channel, status,
                   delivery_type, delivery_duration]
            writer.writerow(row)
            rows.append(row)

            if i % 100000 == 0:
                print(f"  ... {i}/{NUM_ORDERS} orders generated")

    print(f"[OK] ods_order_info.csv — {NUM_ORDERS} orders")

    if write_pg and conn is not None:
        cols = ["order_id", "user_id", "store_id", "product_id",
                "order_date", "order_hour", "quantity", "unit_price",
                "total_amount", "discount_amount", "pay_amount",
                "channel", "status", "delivery_type", "delivery_duration_min"]
        _pg_write_table(conn, PG_ODS_SCHEMA, "ods_order_info", cols, rows)


def generate_inventory(write_pg=False, conn=None):
    filepath = DATA_DIR / "ods_inventory_snapshot.csv"
    rows = []
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["snapshot_date", "store_id", "product_id", "stock_qty",
                          "safety_stock", "reorder_point", "waste_qty",
                          "waste_reason", "promotion_flag"])

        for day_offset in range(0, DATE_RANGE + 1, 1):
            snapshot_date = START_DATE + timedelta(days=day_offset)
            for store_idx in range(1, len(STORES) + 1):
                num_products_in_store = random.randint(60, len(PRODUCT_LIST))
                sampled = random.sample(PRODUCT_LIST, min(num_products_in_store, len(PRODUCT_LIST)))
                for prod in sampled:
                    pid = f"P{PRODUCT_LIST.index(prod)+1:05d}"
                    sid = f"S{store_idx:03d}"
                    base_demand = random.randint(5, 50)
                    stock_qty = max(0, int(random.gauss(base_demand * 3, base_demand)))
                    safety_stock = max(5, int(base_demand * 1.5))
                    reorder_point = max(10, int(base_demand * 2.5))
                    waste_qty = max(0, int(random.gauss(base_demand * 0.08, 2)))
                    waste_reasons = ["过期", "损坏", "滞销", "退货", None]
                    waste_reason = random.choice(waste_reasons) if waste_qty > 0 else None
                    promotion_flag = 1 if random.random() < 0.15 else 0
                    row = [snapshot_date.strftime("%Y-%m-%d"), sid, pid,
                           stock_qty, safety_stock, reorder_point,
                           waste_qty, waste_reason, promotion_flag]
                    writer.writerow(row)
                    rows.append(row)

            if day_offset % 60 == 0:
                print(f"  ... inventory day {day_offset}/{DATE_RANGE}")

    print(f"[OK] ods_inventory_snapshot.csv — {len(rows)} rows")

    if write_pg and conn is not None:
        cols = ["snapshot_date", "store_id", "product_id", "stock_qty",
                "safety_stock", "reorder_point", "waste_qty",
                "waste_reason", "promotion_flag"]
        _pg_write_table(conn, PG_ODS_SCHEMA, "ods_inventory_snapshot", cols, rows)


def generate_user_behavior(write_pg=False, conn=None):
    filepath = DATA_DIR / "ods_user_behavior.csv"
    actions = ["view", "cart", "favorite", "search", "click_banner", "view_detail", "share"]
    total_events = NUM_ORDERS * 3
    rows = []
    with open(filepath, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["event_id", "user_id", "product_id", "action",
                          "event_time", "session_id", "stay_seconds", "page"])

        for i in range(1, total_events + 1):
            eid = f"EVT{uuid.uuid4().hex[:16]}"
            uid = f"U{random.randint(1, NUM_USERS):06d}"
            pid = f"P{random.randint(1, NUM_PRODUCTS):05d}"
            action = random.choice(actions)
            event_date = START_DATE + timedelta(days=random.randint(0, DATE_RANGE))
            event_time = event_date.strftime(
                "%Y-%m-%d") + f" {random.randint(6,23):02d}:{random.randint(0,59):02d}:{random.randint(0,59):02d}"
            session_id = f"SESS{uuid.uuid4().hex[:12]}"
            stay = random.randint(1, 300) if action == "view_detail" else random.randint(1, 60)
            page = random.choice(["首页", "分类页", "搜索页", "商品详情", "购物车", "活动页", "个人中心"])
            row = [eid, uid, pid, action, event_time, session_id, stay, page]
            writer.writerow(row)
            rows.append(row)

            if i % 300000 == 0:
                print(f"  ... {i}/{total_events} behavior events")

    print(f"[OK] ods_user_behavior.csv — {total_events} events")

    if write_pg and conn is not None:
        cols = ["event_id", "user_id", "product_id", "action",
                "event_time", "session_id", "stay_seconds", "page"]
        _pg_write_table(conn, PG_ODS_SCHEMA, "ods_user_behavior", cols, rows)


def main():
    parser = argparse.ArgumentParser(
        description="盒马生鲜模拟数据集生成脚本（可写 PG / 上传至 HDFS）"
    )
    parser.add_argument(
        "--write-pg",
        action="store_true",
        default=False,
        help="生成后是否批量写入 PostgreSQL ods schema（默认: 不写）"
    )
    parser.add_argument(
        "--upload-hdfs",
        action="store_true",
        default=False,
        help="生成后是否上传到 HDFS（默认: 不上传）"
    )
    parser.add_argument(
        "--hdfs-path",
        type=str,
        default="hdfs://192.168.10.128:9000/hema_fresh/ods_raw",
        help="HDFS 目标路径（默认: hdfs://192.168.10.128:9000/hema_fresh/ods_raw）"
    )
    args = parser.parse_args()

    print("=== 盒马生鲜模拟数据集生成 ===")
    print(f"时间范围: {START_DATE.date()} ~ {END_DATE.date()}")
    print(f"生成目录: {DATA_DIR}")
    print(f"写入 PG: {'开启' if args.write_pg else '关闭'}")
    if args.write_pg:
        print(f"  -> host={DB_CONFIG['host']} db={DB_CONFIG['database']} schema={PG_ODS_SCHEMA}")
        print(f"  -> 目标表: {', '.join(PG_ODS_TABLES)}")
    if args.upload_hdfs:
        print(f"HDFS 目标: {args.hdfs_path}")
    else:
        print("HDFS 目标: 不开启（如需上传，请加 --upload-hdfs）")
    print()

    conn = None
    if args.write_pg:
        conn = _get_pg_conn()
        if conn is None:
            print("[PG][ERROR] 无法连接到 PostgreSQL，将跳过 --write-pg 步骤，但仍会生成 CSV")
            args.write_pg = False

    try:
        generate_products(write_pg=args.write_pg, conn=conn)
        generate_stores(write_pg=args.write_pg, conn=conn)
        generate_users(write_pg=args.write_pg, conn=conn)
        generate_orders(write_pg=args.write_pg, conn=conn)
        generate_inventory(write_pg=args.write_pg, conn=conn)
        generate_user_behavior(write_pg=args.write_pg, conn=conn)
    finally:
        if conn is not None:
            try:
                conn.close()
                print("[PG] 连接已关闭")
            except Exception:
                pass

    print()
    print("=== 全部数据集生成完毕 ===")
    print(f"文件列表:")
    for f in sorted(DATA_DIR.glob("*.csv")):
        size_mb = f.stat().st_size / (1024 * 1024)
        print(f"  {f.name:<35s} {size_mb:.2f} MB")

    if args.upload_hdfs:
        print()
        upload_to_hdfs(str(DATA_DIR), args.hdfs_path)


if __name__ == "__main__":
    main()
