SELECT
    db.database_id,
    ti.date_collecte,
    ti.owner_name,
    ti.table_name,
    ti.filegroup_name,
    ti.table_size,
    ti.reserved_size,
    ti.used_size,
    ti.free_size,
    ti.percent_of_db,
    ti.rows_count,
    ti.reserved_memory,
    ti.used_memory,
    ti.number_of_partitions,
    ti.compression_type,
    ti.table_type,
    ti.inserted_at
FROM {{ ref('stg_tablesinfo') }} ti
INNER JOIN {{ ref('dim_databases') }} db
    ON TRIM(LOWER(ti.database_name)) = TRIM(LOWER(db.database_name))
INNER JOIN {{ ref('dim_instances') }} i
    ON db.instance_id = i.instance_id
    AND TRIM(LOWER(ti.instance_name)) = TRIM(LOWER(i.instance_name))
WHERE ti.database_name IS NOT NULL
AND ti.table_name IS NOT NULL
