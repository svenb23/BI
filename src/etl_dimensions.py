# ETL for dim_produkt, dim_zeit and fakt_verkauf

import sqlite3
import os
from datetime import datetime, timedelta

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "output", "dwh.db")


def load_dim_produkt(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("SELECT produkt_id, bezeichnung, produktgruppe, preis FROM stg_produkt")
    staged = cursor.fetchall()

    for produkt_id, bezeichnung, produktgruppe, preis in staged:
        cursor.execute("SELECT sk_produkt FROM dim_produkt WHERE produkt_id = ?", (produkt_id,))
        if not cursor.fetchone():
            cursor.execute("""
                INSERT INTO dim_produkt (produkt_id, bezeichnung, produktgruppe, preis)
                VALUES (?, ?, ?, ?)
            """, (produkt_id, bezeichnung, produktgruppe, preis))

    conn.commit()
    conn.close()
    print(f"dim_produkt: {len(staged)} products loaded")


def load_dim_zeit(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    wochentage = ["Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag", "Sonntag"]

    # generate dates for 2025
    start = datetime(2025, 1, 1)
    end = datetime(2025, 12, 31)
    current = start
    count = 0

    while current <= end:
        datum = current.strftime("%Y-%m-%d")
        cursor.execute("SELECT datum FROM dim_zeit WHERE datum = ?", (datum,))
        if not cursor.fetchone():
            cursor.execute("""
                INSERT INTO dim_zeit (datum, jahr, monat, quartal, tag, wochentag)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                datum,
                current.year,
                current.month,
                (current.month - 1) // 3 + 1,
                current.day,
                wochentage[current.weekday()]
            ))
            count += 1
        current += timedelta(days=1)

    conn.commit()
    conn.close()
    print(f"dim_zeit: {count} dates loaded")


def load_fakt_verkauf(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
        SELECT v.kunden_id, v.produkt_id, v.datum, v.menge, v.umsatz
        FROM stg_verkauf v
    """)
    staged = cursor.fetchall()

    inserted = 0
    for kunden_id, produkt_id, datum, menge, umsatz in staged:
        # lookup surrogate keys (use current active customer)
        cursor.execute(
            "SELECT sk_kunde FROM dim_kunde WHERE kunden_id = ? AND ist_aktuell = 1",
            (kunden_id,)
        )
        sk_kunde = cursor.fetchone()

        cursor.execute(
            "SELECT sk_produkt FROM dim_produkt WHERE produkt_id = ?",
            (produkt_id,)
        )
        sk_produkt = cursor.fetchone()

        if sk_kunde and sk_produkt:
            cursor.execute("""
                INSERT INTO fakt_verkauf (sk_kunde, sk_produkt, datum, menge, umsatz)
                VALUES (?, ?, ?, ?, ?)
            """, (sk_kunde[0], sk_produkt[0], datum, menge, umsatz))
            inserted += 1

    conn.commit()
    conn.close()
    print(f"fakt_verkauf: {inserted} sales loaded")


def show_star_schema(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # sample query: revenue per region
    print("\n--- Revenue per Region ---")
    cursor.execute("""
        SELECT k.region, SUM(v.umsatz) as total, COUNT(*) as orders
        FROM fakt_verkauf v
        JOIN dim_kunde k ON v.sk_kunde = k.sk_kunde
        GROUP BY k.region ORDER BY total DESC
    """)
    print(f"{'Region':<10} {'Umsatz':>10} {'Anzahl':>8}")
    print("-" * 30)
    for row in cursor.fetchall():
        print(f"{row[0]:<10} {row[1]:>10.2f} {row[2]:>8}")

    # sample query: revenue per product group
    print("\n--- Revenue per Product Group ---")
    cursor.execute("""
        SELECT p.produktgruppe, SUM(v.umsatz) as total, COUNT(*) as orders
        FROM fakt_verkauf v
        JOIN dim_produkt p ON v.sk_produkt = p.sk_produkt
        GROUP BY p.produktgruppe ORDER BY total DESC
    """)
    print(f"{'Gruppe':<15} {'Umsatz':>10} {'Anzahl':>8}")
    print("-" * 35)
    for row in cursor.fetchall():
        print(f"{row[0]:<15} {row[1]:>10.2f} {row[2]:>8}")

    # sample query: revenue per month
    print("\n--- Revenue per Month ---")
    cursor.execute("""
        SELECT z.monat, SUM(v.umsatz) as total, COUNT(*) as orders
        FROM fakt_verkauf v
        JOIN dim_zeit z ON v.datum = z.datum
        GROUP BY z.monat ORDER BY z.monat
    """)
    print(f"{'Monat':>6} {'Umsatz':>10} {'Anzahl':>8}")
    print("-" * 26)
    for row in cursor.fetchall():
        print(f"{row[0]:>6} {row[1]:>10.2f} {row[2]:>8}")

    conn.close()


if __name__ == "__main__":
    load_dim_produkt()
    load_dim_zeit()
    load_fakt_verkauf()
    show_star_schema()
