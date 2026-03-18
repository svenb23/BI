# Data quality checks on staging tables before ETL

import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "output", "dwh.db")


def check_nulls(cursor, table, columns):
    issues = 0
    for col in columns:
        cursor.execute(f"SELECT COUNT(*) FROM {table} WHERE {col} IS NULL OR {col} = ''")
        count = cursor.fetchone()[0]
        if count > 0:
            print(f"  WARN: {table}.{col} has {count} NULL/empty values")
            issues += count
    return issues


def check_duplicates(cursor, table, key_column):
    cursor.execute(f"""
        SELECT {key_column}, COUNT(*) as cnt
        FROM {table} GROUP BY {key_column} HAVING cnt > 1
    """)
    dupes = cursor.fetchall()
    for key, cnt in dupes:
        print(f"  WARN: {table}.{key_column} = {key} appears {cnt} times")
    return len(dupes)


def check_referential_integrity(cursor):
    cursor.execute("""
        SELECT COUNT(*) FROM stg_verkauf v
        WHERE NOT EXISTS (SELECT 1 FROM stg_kunde k WHERE k.kunden_id = v.kunden_id)
    """)
    orphan_k = cursor.fetchone()[0]
    if orphan_k > 0:
        print(f"  WARN: {orphan_k} sales reference non-existing customers")

    cursor.execute("""
        SELECT COUNT(*) FROM stg_verkauf v
        WHERE NOT EXISTS (SELECT 1 FROM stg_produkt p WHERE p.produkt_id = v.produkt_id)
    """)
    orphan_p = cursor.fetchone()[0]
    if orphan_p > 0:
        print(f"  WARN: {orphan_p} sales reference non-existing products")

    return orphan_k + orphan_p


def run_checks(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    total = 0

    print("=== Data Quality Checks ===\n")

    print("[1] NULL/empty values:")
    n = check_nulls(cursor, "stg_kunde", ["kunden_id", "name", "stadt", "region", "kategorie"])
    n += check_nulls(cursor, "stg_produkt", ["produkt_id", "bezeichnung", "produktgruppe", "preis"])
    if n == 0:
        print("  OK")
    total += n

    print("\n[2] Duplicates:")
    d = check_duplicates(cursor, "stg_kunde", "kunden_id")
    d += check_duplicates(cursor, "stg_produkt", "produkt_id")
    if d == 0:
        print("  OK")
    total += d

    print("\n[3] Referential integrity:")
    r = check_referential_integrity(cursor)
    if r == 0:
        print("  OK")
    total += r

    print(f"\n=== Result: {total} issue(s) found ===")
    conn.close()
    return total


if __name__ == "__main__":
    run_checks()
