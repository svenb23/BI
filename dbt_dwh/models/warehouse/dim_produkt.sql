-- dimension: products
SELECT
    produkt_id as sk_produkt,
    produkt_id,
    bezeichnung,
    produktgruppe,
    preis
FROM {{ ref('stg_produkte') }}
