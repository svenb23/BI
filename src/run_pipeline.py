# Main pipeline: runs all ETL steps in sequence

import os
import sys

# reset DB before each run
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "output", "dwh.db")

from create_schema import create_schema
from load_staging import load_staging
from etl_scd1 import etl_scd1, show_result as show_scd1
from etl_scd2 import etl_scd2, show_result as show_scd2
from etl_scd3 import etl_scd3, show_result as show_scd3

LOAD_DATES = ["2025-01-01", "2025-02-01", "2025-03-01"]


def reset_db():
    if os.path.exists(DB_PATH):
        os.remove(DB_PATH)
    create_schema(DB_PATH)


def run_scd(scd_type):
    print(f"\n{'='*60}")
    print(f"  SCD TYPE {scd_type}")
    print(f"{'='*60}")

    reset_db()

    for tag in range(1, 4):
        print(f"\n--- Day {tag} ({LOAD_DATES[tag-1]}) ---")
        load_staging(tag, DB_PATH)

        if scd_type == 1:
            etl_scd1(DB_PATH)
        elif scd_type == 2:
            etl_scd2(LOAD_DATES[tag-1], DB_PATH)
        elif scd_type == 3:
            etl_scd3(DB_PATH)

    print(f"\n--- Final result SCD Type {scd_type} ---")
    if scd_type == 1:
        show_scd1(DB_PATH)
    elif scd_type == 2:
        show_scd2(DB_PATH)
    elif scd_type == 3:
        show_scd3(DB_PATH)


if __name__ == "__main__":
    # usage: python src/run_pipeline.py [1|2|3|all]
    arg = sys.argv[1] if len(sys.argv) > 1 else "all"

    if arg == "all":
        for t in [1, 2, 3]:
            run_scd(t)
    else:
        run_scd(int(arg))
