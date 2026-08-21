SELECT
    TRIM(source_instance) AS source_instance,
    date_collecte,
    TRIM(server_name) AS server_name,
    TRIM(instance_name) AS instance_name,
    TRIM(database_name) AS database_name,
    TRIM(log_file_name) AS log_file_name,
    TRIM(filegroup_name) AS filegroup_name,
    TRIM(file_type) AS file_type,
    total_size,
    used_size,
    free_size,
    used_pct,
    free_pct,
    TRIM(autogrow) AS autogrow,
    TRIM(growths_remaining) AS growths_remaining,
    TRIM(max_size) AS max_size,
    TRIM(growth_inc) AS growth_inc,
    TRIM(can_grow) AS can_grow,
    TRIM(file_path) AS file_path,
    inserted_at
FROM {{ source('raw_data', 'raw_logfiles') }}
WHERE database_name IS NOT NULL
AND TRIM(database_name) <> ''
AND log_file_name IS NOT NULL
AND TRIM(log_file_name) <> ''