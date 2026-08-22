SELECT
    backup_id,
    TRIM(source_instance) AS source_instance,
    TRIM(server_name) AS server_name,
    TRIM(instance_name) AS instance_name,
    TRIM(database_name) AS database_name,
    TRIM(recovery_model) AS recovery_model,
    full_start_date,
    full_duration,
    full_size,
    differential_start_date,
    differential_duration,
    differential_size,
    log_start_date,
    log_duration,
    log_size,
    worst_rpo_last_30_days,
    date_collecte,
    inserted_at
FROM {{ source('raw_data', 'raw_backupsdetails') }}
WHERE backup_id IS NOT NULL
AND database_name IS NOT NULL
AND TRIM(database_name) <> ''