# ETL process for SCD Type 1: overwrite changed values (no history)

import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "output", "dwh.db")


def etl_scd1(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # get all customers from staging
    cursor.execute("SELECT kunden_id, name, stadt, region, kategorie FROM stg_kunde")
    staged = cursor.fetchall()

    for kunden_id, name, stadt, region, kategorie in staged:
        # check if customer exists in dim
        cursor.execute(
            "SELECT sk_kunde FROM dim_kunde WHERE kunden_id = ? AND ist_aktuell = 1",
            (kunden_id,)
        )
        existing = cursor.fetchone()

        if existing:
            # SCD1: overwrite all attributes
            cursor.execute("""
                UPDATE dim_kunde
                SET name = ?, stadt = ?, region = ?, kategorie = ?
                WHERE sk_kunde = ?
            """, (name, stadt, region, kategorie, existing[0]))
        else:
            # new customer: insert
            cursor.execute("""
                INSERT INTO dim_kunde
                    (kunden_id, name, stadt, region, kategorie, gueltig_von)
                VALUES (?, ?, ?, ?, ?, DATE('now'))
            """, (kunden_id, name, stadt, region, kategorie))

    conn.commit()
    conn.close()
    print("SCD Type 1 ETL complete")


def show_result(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT sk_kunde, kunden_id, name, stadt, region, kategorie FROM dim_kunde")
    rows = cursor.fetchall()
    conn.close()

    print(f"\n{'SK':<4} {'ID':<4} {'Name':<16} {'Stadt':<12} {'Region':<6} {'Kategorie'}")
    print("-" * 60)
    for row in rows:
        print(f"{row[0]:<4} {row[1]:<4} {row[2]:<16} {row[3]:<12} {row[4]:<6} {row[5]}")


if __name__ == "__main__":
    etl_scd1()
    show_result()
