SELECT
    db.database_id,
    bh.backup_id,
    bh.backup_type,
    bh.start_date,
    bh.duration,
    bh.size_mb,
    bh.copy_only,
    bh.compressed,
    bh.encrypted,
    bh.location,
    bh.date_collecte,
    bh.inserted_at
FROM {{ ref('stg_backuphistory') }} bh
INNER JOIN {{ ref('dim_databases') }} db
    ON TRIM(LOWER(bh.database_name)) = TRIM(LOWER(db.database_name))
INNER JOIN {{ ref('dim_instances') }} i
    ON db.instance_id = i.instance_id
    AND TRIM(LOWER(bh.instance_name)) = TRIM(LOWER(i.instance_name))
WHERE bh.backup_id IS NOT NULL
AND bh.database_name IS NOT NULL
