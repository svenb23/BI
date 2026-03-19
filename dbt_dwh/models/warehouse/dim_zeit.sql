-- dimension: time (generate dates for 2025)
SELECT
    datum::date as datum,
    EXTRACT(YEAR FROM datum)::int as jahr,
    EXTRACT(MONTH FROM datum)::int as monat,
    EXTRACT(QUARTER FROM datum)::int as quartal,
    EXTRACT(DAY FROM datum)::int as tag,
    TO_CHAR(datum, 'Day') as wochentag
FROM generate_series('2025-01-01'::date, '2025-12-31'::date, '1 day') as datum
