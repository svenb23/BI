# Projekt Business Intelligence – Slowly Changing Dimensions (SCD)

Prototypische Umsetzung von ETL-Prozessen am Beispiel der Slowly Changing Dimensions (SCD-Typen 1, 2 und 3).

## Aufgabenstellung

Darstellung der SCD-Typen 1, 2 und 3 anhand konkreter Beispiele mit prototypischer Entwicklung des Datenmodells und der ETL-Prozesse.

## Technologie

- **Sprache:** Python
- **Datenbank:** SQLite
- **ETL:** Python-Skripte

## Projektstruktur

```
BI/
├── src/                  # ETL-Skripte
├── data/                 # Quelldaten (CSV)
├── output/               # Ergebnisse & Visualisierungen
└── README.md
```

## SCD-Typen

| Typ | Strategie | Historie |
|-----|-----------|----------|
| **SCD 1** | Überschreiben | Keine |
| **SCD 2** | Neue Zeile mit Gültigkeitszeitraum | Vollständig |
| **SCD 3** | Zusätzliche Spalte für vorherigen Wert | Begrenzt |

## Kurs

IWBI02 – Projekt Business Intelligence (IU Internationale Hochschule)
