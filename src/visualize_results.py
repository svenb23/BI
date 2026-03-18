# Runs all 3 SCD types and exports a comparison report

import os
import sys

DB_PATH = os.path.join(os.path.dirname(__file__), "..", "output", "dwh.db")
OUTPUT_PATH = os.path.join(os.path.dirname(__file__), "..", "output")

from create_schema import create_schema
from load_staging import load_staging
from etl_scd1 import etl_scd1
from etl_scd2 import etl_scd2
from etl_scd3 import etl_scd3
from quality_checks import run_checks

import pandas as pd
import sqlite3

LOAD_DATES = ["2025-01-01", "2025-02-01", "2025-03-01"]


def reset_db():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    create_schema(DB_PATH)


def run_and_capture(scd_type):
    """Run a full SCD pipeline and return the final dim_kunde as DataFrame."""
    reset_db()
    for tag in range(1, 4):
        load_staging(tag, DB_PATH)
        if scd_type == 1:
            etl_scd1(DB_PATH)
        elif scd_type == 2:
            etl_scd2(LOAD_DATES[tag - 1], DB_PATH)
        elif scd_type == 3:
            etl_scd3(DB_PATH)

    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM dim_kunde ORDER BY kunden_id, sk_kunde", conn)
    conn.close()
    return df


def generate_report():
    print("Running all 3 SCD types...\n")

    results = {}
    for t in [1, 2, 3]:
        results[t] = run_and_capture(t)

    # console output
    for t in [1, 2, 3]:
        print(f"\n{'='*60}")
        print(f"  SCD TYPE {t} - Final Result ({len(results[t])} rows)")
        print(f"{'='*60}")
        print(results[t].to_string(index=False))

    # comparison summary
    print(f"\n{'='*60}")
    print("  COMPARISON")
    print(f"{'='*60}")
    print(f"\n{'Type':<8} {'Rows':<8} {'History':<15} {'Method'}")
    print("-" * 50)
    print(f"{'SCD 1':<8} {len(results[1]):<8} {'None':<15} {'Overwrite'}")
    print(f"{'SCD 2':<8} {len(results[2]):<8} {'Full':<15} {'New row'}")
    print(f"{'SCD 3':<8} {len(results[3]):<8} {'Limited (1)':<15} {'Previous column'}")

    # export to CSV
    for t in [1, 2, 3]:
        path = os.path.join(OUTPUT_PATH, f"result_scd{t}.csv")
        results[t].to_csv(path, index=False)

    print(f"\nCSV exports saved to: {os.path.abspath(OUTPUT_PATH)}")


if __name__ == "__main__":
    generate_report()
