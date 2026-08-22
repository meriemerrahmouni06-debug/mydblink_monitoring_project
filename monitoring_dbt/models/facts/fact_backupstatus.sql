SELECT
    db.database_id,
    bs.database_type,
    bs.last_backup_date,
    bs.backup_status,
    bs.date_collecte,
    bs.inserted_at
FROM {{ ref('stg_backupstatus') }} bs
INNER JOIN {{ ref('dim_databases') }} db
    ON TRIM(LOWER(bs.database_name)) = TRIM(LOWER(db.database_name))
INNER JOIN {{ ref('dim_instances') }} i
    ON db.instance_id = i.instance_id
    AND TRIM(LOWER(bs.instance_name)) = TRIM(LOWER(i.instance_name))
WHERE bs.database_name IS NOT NULL
