# Creates the star schema in a SQLite database

import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "output", "dwh.db")


def create_schema(db_path=DB_PATH):
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    # dim: customer
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_kunde (
            sk_kunde        INTEGER PRIMARY KEY AUTOINCREMENT,
            kunden_id       INTEGER NOT NULL,
            name            TEXT NOT NULL,
            stadt           TEXT NOT NULL,
            region          TEXT NOT NULL,
            kategorie       TEXT NOT NULL,
            gueltig_von     DATE NOT NULL,
            gueltig_bis     DATE NOT NULL DEFAULT '9999-12-31',
            ist_aktuell     INTEGER NOT NULL DEFAULT 1,
            vorherige_stadt TEXT,
            vorherige_region TEXT,
            vorherige_kategorie TEXT
        )
    """)

    # dim: product
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_produkt (
            sk_produkt      INTEGER PRIMARY KEY AUTOINCREMENT,
            produkt_id      INTEGER NOT NULL,
            bezeichnung     TEXT NOT NULL,
            produktgruppe   TEXT NOT NULL,
            preis           REAL NOT NULL
        )
    """)

    # dim: time
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS dim_zeit (
            datum           DATE PRIMARY KEY,
            jahr            INTEGER NOT NULL,
            monat           INTEGER NOT NULL,
            quartal         INTEGER NOT NULL,
            tag             INTEGER NOT NULL,
            wochentag       TEXT NOT NULL
        )
    """)

    # fact: sales
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS fakt_verkauf (
            verkauf_id      INTEGER PRIMARY KEY AUTOINCREMENT,
            sk_kunde        INTEGER NOT NULL,
            sk_produkt      INTEGER NOT NULL,
            datum           DATE NOT NULL,
            menge           INTEGER NOT NULL,
            umsatz          REAL NOT NULL,
            FOREIGN KEY (sk_kunde) REFERENCES dim_kunde(sk_kunde),
            FOREIGN KEY (sk_produkt) REFERENCES dim_produkt(sk_produkt),
            FOREIGN KEY (datum) REFERENCES dim_zeit(datum)
        )
    """)

    conn.commit()
    conn.close()
    print(f"Schema created: {os.path.abspath(db_path)}")


if __name__ == "__main__":
    create_schema()
