# ETL process for SCD Type 3: store previous value in extra column (limited history)

import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "output", "dwh.db")


def etl_scd3(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT kunden_id, name, stadt, region, kategorie FROM stg_kunde")
    staged = cursor.fetchall()

    for kunden_id, name, stadt, region, kategorie in staged:
        cursor.execute(
            "SELECT sk_kunde, stadt, region, kategorie FROM dim_kunde WHERE kunden_id = ? AND ist_aktuell = 1",
            (kunden_id,)
        )
        existing = cursor.fetchone()

        if existing:
            sk, old_stadt, old_region, old_kategorie = existing

            # track changes in "vorherige_" columns
            new_vorherige_stadt = old_stadt if stadt != old_stadt else None
            new_vorherige_region = old_region if region != old_region else None
            new_vorherige_kategorie = old_kategorie if kategorie != old_kategorie else None

            cursor.execute("""
                UPDATE dim_kunde
                SET name = ?, stadt = ?, region = ?, kategorie = ?,
                    vorherige_stadt = COALESCE(?, vorherige_stadt),
                    vorherige_region = COALESCE(?, vorherige_region),
                    vorherige_kategorie = COALESCE(?, vorherige_kategorie)
                WHERE sk_kunde = ?
            """, (name, stadt, region, kategorie,
                  new_vorherige_stadt, new_vorherige_region, new_vorherige_kategorie, sk))
        else:
            # new customer
            cursor.execute("""
                INSERT INTO dim_kunde
                    (kunden_id, name, stadt, region, kategorie, gueltig_von)
                VALUES (?, ?, ?, ?, ?, DATE('now'))
            """, (kunden_id, name, stadt, region, kategorie))

    conn.commit()
    conn.close()
    print("SCD Type 3 ETL complete")


def show_result(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
        SELECT sk_kunde, kunden_id, name, stadt, vorherige_stadt,
               region, vorherige_region, kategorie, vorherige_kategorie
        FROM dim_kunde ORDER BY kunden_id
    """)
    rows = cursor.fetchall()
    conn.close()

    print(f"\n{'SK':<4} {'ID':<4} {'Name':<16} {'Stadt':<12} {'Vorh.Stadt':<12} {'Region':<7} {'Vorh.Reg':<9} {'Kat.':<10} {'Vorh.Kat'}")
    print("-" * 95)
    for r in rows:
        print(f"{r[0]:<4} {r[1]:<4} {r[2]:<16} {r[3]:<12} {str(r[4] or '-'):<12} {r[5]:<7} {str(r[6] or '-'):<9} {r[7]:<10} {str(r[8] or '-')}")


if __name__ == "__main__":
    etl_scd3()
    show_result()
