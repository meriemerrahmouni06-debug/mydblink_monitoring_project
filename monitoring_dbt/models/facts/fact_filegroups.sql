SELECT
    db.database_id,
    fg.date_collecte,
    fg.filegroup_name,
    fg.file_count,
    fg.file_type,
    fg.allocated_size_used,
    fg.total_size_used,
    fg.total_size,
    fg.used_size,
    fg.free_size,
    fg.can_grow,
    fg.inserted_at
FROM {{ ref('stg_filegroups') }} fg
INNER JOIN {{ ref('dim_databases') }} db
    ON TRIM(LOWER(fg.database_name)) = TRIM(LOWER(db.database_name))
INNER JOIN {{ ref('dim_instances') }} i
    ON db.instance_id = i.instance_id
    AND TRIM(LOWER(fg.instance_name)) = TRIM(LOWER(i.instance_name))
WHERE fg.database_name IS NOT NULL
AND fg.filegroup_name IS NOT NULL
