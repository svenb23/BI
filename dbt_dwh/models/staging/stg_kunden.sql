-- staging: current customer data (latest day)
SELECT
    kunden_id,
    name,
    stadt,
    region,
    kategorie
FROM {{ ref('kunden_tag3') }}
