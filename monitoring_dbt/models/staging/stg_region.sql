SELECT
    region_id,
    TRIM(source_instance) AS source_instance,
    date_collecte,
    TRIM(server_name) AS server_name,
    TRIM(ip_address) AS ip_address,
    TRIM(region) AS region,
    TRIM(country) AS country,
    TRIM(city) AS city,
    latitude,
    longitude,
    inserted_at
FROM {{ source('raw_data', 'raw_region') }}
WHERE server_name IS NOT NULL
AND TRIM(server_name) <> ''