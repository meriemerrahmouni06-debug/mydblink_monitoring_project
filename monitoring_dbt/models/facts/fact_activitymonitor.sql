SELECT
    i.instance_id,
    db.database_id,
    am.activity_id,
    am.date_collecte,
    am.session_id,
    am.user_process,
    am.user_name,
    am.application_name,
    am.host_name,
    am.wait_type,
    am.statement_text,
    am.inserted_at
FROM {{ ref('stg_activitymonitor') }} am
INNER JOIN {{ ref('dim_instances') }} i
    ON TRIM(LOWER(am.instance_name)) = TRIM(LOWER(i.instance_name))
LEFT JOIN {{ ref('dim_databases') }} db
    ON TRIM(LOWER(am.database_name)) = TRIM(LOWER(db.database_name))
    AND db.instance_id = i.instance_id
WHERE am.activity_id IS NOT NULL
