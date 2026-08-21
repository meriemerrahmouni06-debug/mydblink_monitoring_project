SELECT
    disk_id,
    TRIM(source_instance) AS source_instance,
    TRIM(drive_name) AS drive_name,
    TRIM(instance_name) AS instance_name,
    TRIM(physical_netbios_name) AS physical_netbios_name,
    total_space_gb,
    free_space_gb,
    date_collecte,
    date_collecte2,
    inserted_at
FROM {{ source('raw_data', 'raw_diskspace') }}
WHERE disk_id IS NOT NULL