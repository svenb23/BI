-- fact: sales
SELECT
    ROW_NUMBER() OVER () as verkauf_id,
    v.kunden_id as sk_kunde,
    v.produkt_id as sk_produkt,
    v.datum,
    v.menge,
    v.umsatz
FROM {{ ref('stg_verkauefe') }} v
