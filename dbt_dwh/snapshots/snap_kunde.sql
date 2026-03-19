-- SCD Type 2: dbt snapshot tracks changes automatically
{% snapshot snap_kunde %}

{{
    config(
        target_schema='public',
        unique_key='kunden_id',
        strategy='check',
        check_cols=['name', 'stadt', 'region', 'kategorie']
    )
}}

SELECT
    kunden_id,
    name,
    stadt,
    region,
    kategorie
FROM {{ ref('stg_kunden') }}

{% endsnapshot %}
