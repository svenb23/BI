# Loads CSV source data into staging tables

import pandas as pd
import sqlite3
import os

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "output", "dwh.db")
DATA_PATH = os.path.join(os.path.dirname(__file__), "..", "data")


def load_staging(tag, db_path=DB_PATH):
    conn = sqlite3.connect(db_path)

    # staging: customers
    kunden = pd.read_csv(os.path.join(DATA_PATH, f"kunden_tag{tag}.csv"))
    kunden.to_sql("stg_kunde", conn, if_exists="replace", index=False)
    print(f"Staging: {len(kunden)} customers loaded (day {tag})")

    # staging: products (same every day)
    produkte = pd.read_csv(os.path.join(DATA_PATH, "produkte.csv"))
    produkte.to_sql("stg_produkt", conn, if_exists="replace", index=False)
    print(f"Staging: {len(produkte)} products loaded")

    # staging: sales (same every day)
    verkauefe = pd.read_csv(os.path.join(DATA_PATH, "verkauefe.csv"))
    verkauefe.to_sql("stg_verkauf", conn, if_exists="replace", index=False)
    print(f"Staging: {len(verkauefe)} sales loaded")

    conn.close()


if __name__ == "__main__":
    import sys
    tag = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    load_staging(tag)
    print(f"\nStaging load complete (day {tag})")
