SELECT 
    d.database_id,
    d.database_name,
    i.instance_id
FROM {{ref('stg_databases')}} d INNER JOIN {{ref('dim_instances')}} i
ON TRIM(LOWER(d.instance_name)) =TRIM(LOWER(i.instance_name)) AND d.database_id IS NOT NULL AND 
d.database_name IS NOT NULL 
