SELECT
    TRIM(source_instance) AS source_instance,
    date_collecte,
    TRIM(server_name) AS server_name,
    TRIM(instance_name) AS instance_name,
    TRIM(database_name) AS database_name,
    TRIM(owner_name) AS owner_name,
    TRIM(table_name) AS table_name,
    TRIM(filegroup_name) AS filegroup_name,
    table_size,
    reserved_size,
    used_size,
    free_size,
    percent_of_db,
    rows_count,
    reserved_memory,
    used_memory,
    number_of_partitions,
    TRIM(compression_type) AS compression_type,
    TRIM(table_type) AS table_type,
    inserted_at
FROM {{ source('raw_data', 'raw_tablesinfo') }}
WHERE database_name IS NOT NULL
AND TRIM(database_name) <> ''
AND table_name IS NOT NULL
AND TRIM(table_name) <> ''