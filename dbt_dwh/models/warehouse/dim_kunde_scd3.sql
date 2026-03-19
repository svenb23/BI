-- SCD Type 3: previous value in extra column
-- compares tag1 (previous) with current staging to track changes
SELECT
    curr.kunden_id as sk_kunde,
    curr.kunden_id,
    curr.name,
    curr.stadt,
    CASE WHEN curr.stadt != prev.stadt THEN prev.stadt END as vorherige_stadt,
    curr.region,
    CASE WHEN curr.region != prev.region THEN prev.region END as vorherige_region,
    curr.kategorie,
    CASE WHEN curr.kategorie != prev.kategorie THEN prev.kategorie END as vorherige_kategorie
FROM {{ ref('stg_kunden') }} curr
LEFT JOIN {{ ref('kunden_tag1') }} prev
    ON curr.kunden_id = prev.kunden_id
