# Generates CSV source files simulating an operational system over 3 days

import pandas as pd
import os

DATA_PATH = os.path.join(os.path.dirname(__file__), "..", "data")


def generate_kunden():
    # day 1: initial load
    tag1 = pd.DataFrame([
        {"kunden_id": 1, "name": "Maxx Müller",  "stadt": "Berlin",    "region": "Ost",  "kategorie": "Standard"},
        {"kunden_id": 2, "name": "Anna Schmidt",  "stadt": "Hamburg",   "region": "Nord", "kategorie": "Premium"},
        {"kunden_id": 3, "name": "Tom Weber",     "stadt": "München",   "region": "Süd",  "kategorie": "Standard"},
        {"kunden_id": 4, "name": "Lisa Fischer",  "stadt": "Köln",      "region": "West", "kategorie": "Standard"},
        {"kunden_id": 5, "name": "Jan Hoffmann",  "stadt": "Dresden",   "region": "Ost",  "kategorie": "Premium"},
    ])

    # day 2: name fix (SCD1), relocation (SCD2), category upgrade (SCD3)
    tag2 = pd.DataFrame([
        {"kunden_id": 1, "name": "Max Müller",    "stadt": "Berlin",    "region": "Ost",  "kategorie": "Standard"},
        {"kunden_id": 2, "name": "Anna Schmidt",  "stadt": "Hamburg",   "region": "Nord", "kategorie": "Premium"},
        {"kunden_id": 3, "name": "Tom Weber",     "stadt": "Frankfurt", "region": "West", "kategorie": "Standard"},
        {"kunden_id": 4, "name": "Lisa Fischer",  "stadt": "Köln",      "region": "West", "kategorie": "Premium"},
        {"kunden_id": 5, "name": "Jan Hoffmann",  "stadt": "Dresden",   "region": "Ost",  "kategorie": "Premium"},
    ])

    # day 3: another relocation (SCD2), name fix (SCD1), category upgrade (SCD3)
    tag3 = pd.DataFrame([
        {"kunden_id": 1, "name": "Max Müller",    "stadt": "Stuttgart", "region": "Süd",  "kategorie": "Standard"},
        {"kunden_id": 2, "name": "Anna Schmidt",  "stadt": "Hamburg",   "region": "Nord", "kategorie": "Gold"},
        {"kunden_id": 3, "name": "Tom Weber",     "stadt": "Frankfurt", "region": "West", "kategorie": "Standard"},
        {"kunden_id": 4, "name": "Lisa Fischer",  "stadt": "Köln",      "region": "West", "kategorie": "Premium"},
        {"kunden_id": 5, "name": "Jann Hoffmann", "stadt": "Dresden",   "region": "Ost",  "kategorie": "Premium"},
    ])

    tag1.to_csv(os.path.join(DATA_PATH, "kunden_tag1.csv"), index=False)
    tag2.to_csv(os.path.join(DATA_PATH, "kunden_tag2.csv"), index=False)
    tag3.to_csv(os.path.join(DATA_PATH, "kunden_tag3.csv"), index=False)
    print("Customer CSVs created (day 1-3)")


def generate_produkte():
    produkte = pd.DataFrame([
        {"produkt_id": 1, "bezeichnung": "Laptop",      "produktgruppe": "Elektronik", "preis": 999.99},
        {"produkt_id": 2, "bezeichnung": "Maus",         "produktgruppe": "Elektronik", "preis": 29.99},
        {"produkt_id": 3, "bezeichnung": "Schreibtisch", "produktgruppe": "Möbel",      "preis": 249.99},
        {"produkt_id": 4, "bezeichnung": "Monitor",      "produktgruppe": "Elektronik", "preis": 449.99},
        {"produkt_id": 5, "bezeichnung": "Bürostuhl",    "produktgruppe": "Möbel",      "preis": 189.99},
    ])
    produkte.to_csv(os.path.join(DATA_PATH, "produkte.csv"), index=False)
    print("Product CSV created")


def generate_verkauefe():
    verkauefe = pd.DataFrame([
        {"kunden_id": 1, "produkt_id": 1, "datum": "2025-01-15", "menge": 1, "umsatz": 999.99},
        {"kunden_id": 2, "produkt_id": 2, "datum": "2025-01-16", "menge": 3, "umsatz": 89.97},
        {"kunden_id": 3, "produkt_id": 3, "datum": "2025-01-17", "menge": 1, "umsatz": 249.99},
        {"kunden_id": 1, "produkt_id": 4, "datum": "2025-02-01", "menge": 2, "umsatz": 899.98},
        {"kunden_id": 4, "produkt_id": 5, "datum": "2025-02-10", "menge": 1, "umsatz": 189.99},
        {"kunden_id": 5, "produkt_id": 1, "datum": "2025-02-15", "menge": 1, "umsatz": 999.99},
        {"kunden_id": 2, "produkt_id": 3, "datum": "2025-03-01", "menge": 1, "umsatz": 249.99},
        {"kunden_id": 3, "produkt_id": 2, "datum": "2025-03-05", "menge": 5, "umsatz": 149.95},
    ])
    verkauefe.to_csv(os.path.join(DATA_PATH, "verkauefe.csv"), index=False)
    print("Sales CSV created")


if __name__ == "__main__":
    os.makedirs(DATA_PATH, exist_ok=True)
    generate_kunden()
    generate_produkte()
    generate_verkauefe()
    print(f"\nAll test data created in: {os.path.abspath(DATA_PATH)}")
