SELECT
    server_id,
    server_name
FROM {{ ref('stg_servers') }}
WHERE server_id IS NOT NULL
AND server_name IS NOT NULL