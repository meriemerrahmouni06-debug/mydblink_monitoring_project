SELECT
    s.server_id,
    r.region_id,
    r.date_collecte,
    r.ip_address,
    r.region,
    r.country,
    r.city,
    r.latitude,
    r.longitude,
    r.inserted_at
FROM {{ ref('stg_region') }} r
INNER JOIN {{ ref('dim_servers') }} s
    ON TRIM(LOWER(r.server_name)) = TRIM(LOWER(s.server_name))
WHERE r.server_name IS NOT NULL
