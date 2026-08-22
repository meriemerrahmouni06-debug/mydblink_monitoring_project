SELECT
    db.database_id,
    lf.date_collecte,
    lf.log_file_name,
    lf.filegroup_name,
    lf.file_type,
    lf.total_size,
    lf.used_size,
    lf.free_size,
    lf.used_pct,
    lf.free_pct,
    lf.autogrow,
    lf.growths_remaining,
    lf.max_size,
    lf.growth_inc,
    lf.can_grow,
    lf.file_path,
    lf.inserted_at
FROM {{ ref('stg_logfiles') }} lf
INNER JOIN {{ ref('dim_databases') }} db
    ON TRIM(LOWER(lf.database_name)) = TRIM(LOWER(db.database_name))
INNER JOIN {{ ref('dim_instances') }} i
    ON db.instance_id = i.instance_id
    AND TRIM(LOWER(lf.instance_name)) = TRIM(LOWER(i.instance_name))
WHERE lf.database_name IS NOT NULL
AND lf.log_file_name IS NOT NULL
