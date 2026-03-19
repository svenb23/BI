-- staging: product data
SELECT
    produkt_id,
    bezeichnung,
    produktgruppe,
    preis
FROM {{ ref('produkte') }}
