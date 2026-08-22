SELECT
    db.database_id,
    df.datafile_id,
    df.date_collecte,
    df.date_collecte2,
    df.physical_netbios_name,
    df.logical_data_name,
    df.file_location,
    df.total_filesize_mb,
    df.file_type,
    df.filegroup_name,
    df.inserted_at
FROM {{ ref('stg_datafiles') }} df
INNER JOIN {{ ref('dim_databases') }} db
    ON TRIM(LOWER(df.database_name)) = TRIM(LOWER(db.database_name))
INNER JOIN {{ ref('dim_instances') }} i
    ON db.instance_id = i.instance_id
    AND TRIM(LOWER(df.instance_name)) = TRIM(LOWER(i.instance_name))
WHERE df.datafile_id IS NOT NULL
AND df.database_name IS NOT NULL
