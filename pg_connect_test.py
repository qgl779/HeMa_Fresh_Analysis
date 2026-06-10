#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
盒马生鲜数仓 - PostgreSQL 连接与数据完整性检查脚本
适用：PostgreSQL 15 / Python 3.6+（仅使用标准字符串格式化，不使用 f-string）
功能：
    1) 测试 psycopg2 连接 192.168.10.144:5432
    2) 打印 hema_fresh_dw 数据库版本
    3) 检查 ods / dwd / dws / ads 各 schema 是否存在
    4) 统计每个 schema 下的表数
依赖：pip install psycopg2-binary
"""
from __future__ import print_function

import sys

# ---------- 数据库配置 ----------
DB_HOST = "192.168.10.144"
DB_PORT = 5432
DB_USER = "hema_admin"
DB_PASSWORD = "hema2024"
DB_NAME = "hema_fresh_dw"
TARGET_SCHEMAS = ["ods", "dwd", "dws", "ads"]


def banner(title):
    print("")
    print("=" * 68)
    print("  " + title)
    print("=" * 68)


def check_psycopg2():
    try:
        import psycopg2  # noqa: F401
        import psycopg2.extras  # noqa: F401
        print("[OK] psycopg2 模块可用")
        return True
    except ImportError as e:
        print("[ERR] 缺少 psycopg2 模块，请先执行: pip install psycopg2-binary")
        print("      错误详情: %s" % str(e))
        return False


def main():
    banner("盒马生鲜数仓 - PostgreSQL 连接测试")

    if not check_psycopg2():
        sys.exit(1)

    import psycopg2
    from psycopg2 import sql

    conn = None
    try:
        # 1) 连接
        print("正在连接 %s:%s/%s ..." % (DB_HOST, DB_PORT, DB_NAME))
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            user=DB_USER,
            password=DB_PASSWORD,
            dbname=DB_NAME,
            connect_timeout=10,
        )
        print("[OK] 连接成功！")
        print("      用户: %s    数据库: %s" % (DB_USER, DB_NAME))

        cur = conn.cursor()

        # 2) 打印数据库版本
        banner("1) PostgreSQL 版本")
        cur.execute("SELECT version();")
        version_full = cur.fetchone()[0]
        print("%s" % version_full)

        cur.execute("SHOW server_version;")
        version_short = cur.fetchone()[0]
        print("      server_version: %s" % version_short)

        # 3) 检查各 schema 是否存在
        banner("2) Schema 存在性检查 (%s)" % ", ".join(TARGET_SCHEMAS))
        cur.execute(
            "SELECT schema_name FROM information_schema.schemata "
            "WHERE schema_name = ANY(%s);",
            (TARGET_SCHEMAS,),
        )
        found = [row[0] for row in cur.fetchall()]

        for sch in TARGET_SCHEMAS:
            if sch in found:
                print("  [OK] schema '%s' 存在" % sch)
            else:
                print("  [WARN] schema '%s' 不存在 (请执行: CREATE SCHEMA IF NOT EXISTS %s;)" % (sch, sch))

        # 4) 统计每个 schema 下的表数
        banner("3) 各 schema 表数统计")
        cur.execute(
            "SELECT table_schema, COUNT(*) FROM information_schema.tables "
            "WHERE table_schema = ANY(%s) AND table_type = 'BASE TABLE' "
            "GROUP BY table_schema ORDER BY table_schema;",
            (TARGET_SCHEMAS,),
        )
        rows = cur.fetchall()
        row_map = {row[0]: row[1] for row in rows}

        print("%-12s | %8s | 表列表" % ("SCHEMA", "TABLES"))
        print("-" * 68)
        total = 0
        for sch in TARGET_SCHEMAS:
            cnt = row_map.get(sch, 0)
            total += cnt
            # 查前 8 个表名做简要展示
            cur.execute(
                "SELECT table_name FROM information_schema.tables "
                "WHERE table_schema = %s AND table_type = 'BASE TABLE' "
                "ORDER BY table_name LIMIT 8;",
                (sch,),
            )
            tbls = [r[0] for r in cur.fetchall()]
            preview = ", ".join(tbls[:8])
            if len(tbls) >= 8:
                preview += " ..."
            print("%-12s | %8d | %s" % (sch, cnt, preview or "(无表)"))
        print("-" * 68)
        print("%-12s | %8d" % ("TOTAL", total))

        # 5) 表行数概览（按 schema 汇总）
        banner("4) 各表行数概览（估算值，由 pg_class.reltuples）")
        cur.execute(
            "SELECT n.nspname AS schema_name, c.relname AS table_name, "
            "c.reltuples::BIGINT AS estimated_rows "
            "FROM pg_class c "
            "JOIN pg_namespace n ON n.oid = c.relnamespace "
            "WHERE n.nspname = ANY(%s) "
            "AND c.relkind = 'r' "
            "ORDER BY schema_name, estimated_rows DESC;",
            (TARGET_SCHEMAS,),
        )
        rows = cur.fetchall()
        print("%-10s | %-38s | %12s" % ("SCHEMA", "TABLE", "EST_ROWS"))
        print("-" * 68)
        for sch, tbl, est in rows:
            est_int = 0 if est is None else int(est)
            print("%-10s | %-38s | %12d" % (sch, tbl, est_int))

        cur.close()
        print("")
        print("[OK] PostgreSQL 连接测试全部完成 ✓")

    except psycopg2.OperationalError as e:
        print("[ERR] 连接失败: %s" % str(e))
        print("      可能原因: PostgreSQL 未启动 / 防火墙拦截 / host/port 错误 / pg_hba.conf 未允许此 IP")
        sys.exit(1)
    except psycopg2.Error as e:
        print("[ERR] PostgreSQL 错误: %s" % str(e))
        sys.exit(1)
    finally:
        if conn is not None:
            conn.close()
            print("      连接已关闭")


if __name__ == "__main__":
    main()
