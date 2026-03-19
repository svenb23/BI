-- staging: sales data
SELECT
    kunden_id,
    produkt_id,
    datum::date as datum,
    menge,
    umsatz
FROM {{ ref('verkauefe') }}
