SELECT
    i.instance_id,
    ss.service_id,
    ss.service_name,
    ss.status,
    ss.date_collecte,
    ss.inserted_at
FROM {{ ref('stg_servicestatus') }} ss
INNER JOIN {{ ref('dim_instances') }} i
    ON TRIM(LOWER(ss.instance_name)) = TRIM(LOWER(i.instance_name))
WHERE ss.service_id IS NOT NULL
AND ss.service_name IS NOT NULL
