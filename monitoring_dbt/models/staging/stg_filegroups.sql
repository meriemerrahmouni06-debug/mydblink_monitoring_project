SELECT
    TRIM(source_instance) AS source_instance,
    date_collecte,
    TRIM(server_name) AS server_name,
    TRIM(instance_name) AS instance_name,
    TRIM(database_name) AS database_name,
    TRIM(filegroup_name) AS filegroup_name,
    file_count,
    TRIM(file_type) AS file_type,
    allocated_size_used,
    total_size_used,
    total_size,
    used_size,
    free_size,
    TRIM(can_grow) AS can_grow,
    inserted_at
FROM {{ source('raw_data', 'raw_filegroups') }}
WHERE database_name IS NOT NULL
AND TRIM(database_name) <> ''
AND filegroup_name IS NOT NULL
AND TRIM(filegroup_name) <> ''