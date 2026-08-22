SELECT
    i.instance_id,
    m.memory_id,
    m.total_osmemory,
    m.avalaible_memory,
    m.date_collecte,
    m.date_collecte2,
    m.inserted_at
FROM {{ ref('stg_memory') }} m
INNER JOIN {{ ref('dim_instances') }} i
    ON TRIM(LOWER(m.instance_name)) = TRIM(LOWER(i.instance_name))
WHERE m.memory_id IS NOT NULL
