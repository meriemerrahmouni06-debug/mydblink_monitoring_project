SELECT
    activity_id,
    TRIM(source_instance) AS source_instance,
    date_collecte,
    TRIM(server_name) AS server_name,
    TRIM(instance_name) AS instance_name,
    session_id,
    user_process,
    TRIM(user_name) AS user_name,
    TRIM(database_name) AS database_name,
    TRIM(application_name) AS application_name,
    TRIM(host_name) AS host_name,
    TRIM(wait_type) AS wait_type,
    statement_text,
    inserted_at
FROM {{ source('raw_data', 'raw_activitymonitor') }}
WHERE activity_id IS NOT NULL