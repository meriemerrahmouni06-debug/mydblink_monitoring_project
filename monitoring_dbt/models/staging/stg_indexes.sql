SELECT
    TRIM(source_instance) AS source_instance,
    TRIM(server_name) AS server_name,
    TRIM(instance_name) AS instance_name,
    TRIM(database_name) AS database_name,
    TRIM(owner_name) AS owner_name,
    TRIM(table_name) AS table_name,
    TRIM(index_name) AS index_name,
    index_id,
    TRIM(filegroup_name) AS filegroup_name,
    TRIM(index_type) AS index_type,
    no_of_keys,
    index_size,
    used_size,
    free_size,
    rows_count,
    row_mod_ctr,
    original_fill_factor,
    date_collecte,
    inserted_at
FROM {{ source('raw_data', 'raw_indexes') }}
WHERE database_name IS NOT NULL
AND TRIM(database_name) <> ''
AND table_name IS NOT NULL
AND TRIM(table_name) <> ''
AND index_name IS NOT NULL
AND TRIM(index_name) <> ''