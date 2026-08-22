SELECT
    db.database_id,
    bd.backup_id,
    bd.recovery_model,
    bd.full_start_date,
    bd.full_duration,
    bd.full_size,
    bd.differential_start_date,
    bd.differential_duration,
    bd.differential_size,
    bd.log_start_date,
    bd.log_duration,
    bd.log_size,
    bd.worst_rpo_last_30_days,
    bd.date_collecte,
    bd.inserted_at
FROM {{ ref('stg_backupdetails') }} bd
INNER JOIN {{ ref('dim_databases') }} db
    ON TRIM(LOWER(bd.database_name)) = TRIM(LOWER(db.database_name))
INNER JOIN {{ ref('dim_instances') }} i
    ON db.instance_id = i.instance_id
    AND TRIM(LOWER(bd.instance_name)) = TRIM(LOWER(i.instance_name))
WHERE bd.backup_id IS NOT NULL
AND bd.database_name IS NOT NULL
