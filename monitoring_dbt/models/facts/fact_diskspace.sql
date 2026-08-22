SELECT
    i.instance_id,
    d.disk_id,
    d.drive_name,
    d.physical_netbios_name,
    d.total_space_gb,
    d.free_space_gb,
    d.date_collecte,
    d.date_collecte2,
    d.inserted_at
FROM {{ ref('stg_diskspace') }} d
INNER JOIN {{ ref('dim_instances') }} i
    ON TRIM(LOWER(d.instance_name)) = TRIM(LOWER(i.instance_name))
WHERE d.disk_id IS NOT NULL
