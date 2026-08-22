SELECT
    i.instance_id,
    c.cpu_id,
    c.cpu_idle,
    c.cpu_sql,
    c.date_collecte,
    c.date_collecte2,
    c.inserted_at
FROM {{ ref('stg_cpu') }} c
INNER JOIN {{ ref('dim_instances') }} i
    ON TRIM(LOWER(c.instance_name)) = TRIM(LOWER(i.instance_name))
WHERE c.cpu_id IS NOT NULL
