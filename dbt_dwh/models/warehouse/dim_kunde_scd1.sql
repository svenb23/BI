-- SCD Type 1: overwrite (always latest values, no history)
SELECT
    kunden_id as sk_kunde,
    kunden_id,
    name,
    stadt,
    region,
    kategorie
FROM {{ ref('stg_kunden') }}
