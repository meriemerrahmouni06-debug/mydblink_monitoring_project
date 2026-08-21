SELECT
    TRIM(source_instance) AS source_instance,
    TRIM(server_name) AS server_name,
    TRIM(instance_name) AS instance_name,
    TRIM(database_name) AS database_name,
    TRIM(database_type) AS database_type,
    last_backup_date,
    TRIM(backup_status) AS backup_status,
    date_collecte,
    inserted_at
FROM {{ source('raw_data', 'raw_backupstatus') }}
WHERE database_name IS NOT NULL
AND TRIM(database_name) <> ''