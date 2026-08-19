SELECT 
    i.instance_id,
    i.instance_name,
    s.server_id
    FROM {{ref('stg_instances')}} i 
    INNER JOIN {{ref('dim_servers')}} s
    ON TRIM(LOWER(i.server_name)) = TRIM(LOWER(s.server_name)) AND
    i.instance_id IS NOT NULL AND i.instance_name IS NOT NULL