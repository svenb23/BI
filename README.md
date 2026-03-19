# Projekt Business Intelligence – Slowly Changing Dimensions (SCD)

Prototypische Umsetzung von ETL-Prozessen am Beispiel der Slowly Changing Dimensions (SCD-Typen 1, 2 und 3).

## Aufgabenstellung

Darstellung der SCD-Typen 1, 2 und 3 anhand konkreter Beispiele mit prototypischer Entwicklung des Datenmodells und der ETL-Prozesse.

## Technologie

### Prototyp (Python + SQLite)

| Komponente | Technologie |
|-----------|-------------|
| Sprache | Python 3.13 |
| Datenbank | SQLite |
| ETL | Python-Skripte |

### Professioneller Stack (dbt + PostgreSQL)

| Komponente | Technologie |
|-----------|-------------|
| Transformation | dbt 1.11 |
| Datenbank | PostgreSQL 17 |
| Datenqualität | dbt Tests |
| SCD Typ 2 | dbt Snapshots |

## Projektstruktur

```
BI/
├── src/                          # Python-Prototyp
│   ├── create_schema.py          # Star-Schema anlegen
│   ├── generate_testdata.py      # CSV-Quelldaten erzeugen
│   ├── load_staging.py           # Staging Area befüllen
│   ├── etl_scd1.py               # SCD Typ 1 (Überschreiben)
│   ├── etl_scd2.py               # SCD Typ 2 (Neue Zeile)
│   ├── etl_scd3.py               # SCD Typ 3 (Neue Spalte)
│   ├── etl_dimensions.py         # Produkt-, Zeit-, Faktentabelle
│   ├── quality_checks.py         # Datenqualitätsprüfungen
│   ├── run_pipeline.py           # Gesamte Pipeline ausführen
│   └── visualize_results.py      # SCD-Vergleich & CSV-Export
├── data/                         # Quelldaten (CSV)
├── output/                       # Ergebnisse
├── dbt_dwh/                      # dbt-Projekt
│   ├── seeds/                    # CSV-Quelldaten für dbt
│   ├── models/
│   │   ├── staging/              # Staging Views + Tests
│   │   └── warehouse/            # Dimensionen + Fakten
│   └── snapshots/                # SCD Typ 2 (dbt Snapshot)
└── README.md
```

## Ausführung

### Prototyp (Python)

```bash
# Setup
python -m venv venv
source venv/bin/activate  # Windows: venv\Scripts\activate
pip install -r requirements.txt

# Testdaten generieren
python src/generate_testdata.py

# Gesamte Pipeline ausführen (alle 3 SCD-Typen)
cd src
python run_pipeline.py all

# Nur einen SCD-Typ ausführen
python run_pipeline.py 1   # SCD Typ 1
python run_pipeline.py 2   # SCD Typ 2
python run_pipeline.py 3   # SCD Typ 3

# Vergleichsreport
python visualize_results.py
```

### Professioneller Stack (dbt)

```bash
# Voraussetzung: PostgreSQL läuft, Datenbank bi_dwh existiert
# ~/.dbt/profiles.yml muss konfiguriert sein

cd dbt_dwh

dbt seed          # CSVs in PostgreSQL laden
dbt run           # Models erstellen
dbt snapshot      # SCD Typ 2 Historisierung
dbt test          # Datenqualitätstests
```

## SCD-Typen

| Typ | Strategie | Historie | Ergebnis (5 Kunden, 3 Tage) |
|-----|-----------|----------|-----------------------------|
| **SCD 1** | Überschreiben | Keine | 5 Zeilen |
| **SCD 2** | Neue Zeile mit Gültigkeitszeitraum | Vollständig | 11 Zeilen |
| **SCD 3** | Zusätzliche Spalte für vorherigen Wert | Begrenzt (1 Vorgänger) | 5 Zeilen |

## Kurs

IWBI02 – Projekt Business Intelligence (IU Internationale Hochschule)
