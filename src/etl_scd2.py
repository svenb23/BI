# ETL process for SCD Type 2: add new row on change (full history)

import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "output", "dwh.db")


def etl_scd2(load_date, db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT kunden_id, name, stadt, region, kategorie FROM stg_kunde")
    staged = cursor.fetchall()

    for kunden_id, name, stadt, region, kategorie in staged:
        cursor.execute(
            "SELECT sk_kunde, name, stadt, region, kategorie FROM dim_kunde WHERE kunden_id = ? AND ist_aktuell = 1",
            (kunden_id,)
        )
        existing = cursor.fetchone()

        if existing:
            sk, old_name, old_stadt, old_region, old_kategorie = existing

            # check if anything changed
            if (name, stadt, region, kategorie) != (old_name, old_stadt, old_region, old_kategorie):
                # close old record
                cursor.execute("""
                    UPDATE dim_kunde
                    SET gueltig_bis = ?, ist_aktuell = 0
                    WHERE sk_kunde = ?
                """, (load_date, sk))

                # insert new record
                cursor.execute("""
                    INSERT INTO dim_kunde
                        (kunden_id, name, stadt, region, kategorie, gueltig_von)
                    VALUES (?, ?, ?, ?, ?, ?)
                """, (kunden_id, name, stadt, region, kategorie, load_date))
        else:
            # new customer
            cursor.execute("""
                INSERT INTO dim_kunde
                    (kunden_id, name, stadt, region, kategorie, gueltig_von)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (kunden_id, name, stadt, region, kategorie, load_date))

    conn.commit()
    conn.close()
    print(f"SCD Type 2 ETL complete (date: {load_date})")


def show_result(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT sk_kunde, kunden_id, name, stadt, region, kategorie,
               gueltig_von, gueltig_bis, ist_aktuell
        FROM dim_kunde ORDER BY kunden_id, gueltig_von
    """)
    rows = cursor.fetchall()
    conn.close()

    print(f"\n{'SK':<4} {'ID':<4} {'Name':<16} {'Stadt':<12} {'Region':<6} {'Kat.':<10} {'Von':<12} {'Bis':<12} {'Akt.'}")
    print("-" * 90)
    for r in rows:
        print(f"{r[0]:<4} {r[1]:<4} {r[2]:<16} {r[3]:<12} {r[4]:<6} {r[5]:<10} {r[6]:<12} {r[7]:<12} {r[8]}")


if __name__ == "__main__":
    import sys
    load_date = sys.argv[1] if len(sys.argv) > 1 else "2025-01-01"
    etl_scd2(load_date)
    show_result()
