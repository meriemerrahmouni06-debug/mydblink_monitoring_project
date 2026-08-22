SELECT
    TRIM(source_instance) AS source_instance,
    date_collecte,
    date_collecte2,
    TRIM(physical_netbios_name) AS physical_netbios_name,
    TRIM(instance_name) AS instance_name,
    TRIM(database_name) AS database_name,
    TRIM(full_obj_name) AS full_obj_name,
    index_id,
    TRIM(index_name) AS index_name,
    TRIM(index_type_desc) AS index_type_desc,
    index_depth,
    index_level,
    avg_fragmentation,
    fragment_count,
    rank,
    inserted_at
FROM {{ source('raw_data', 'raw_fragmentation') }}
WHERE database_name IS NOT NULL
AND TRIM(database_name) <> ''
AND full_obj_name IS NOT NULL
AND TRIM(full_obj_name) <> ''