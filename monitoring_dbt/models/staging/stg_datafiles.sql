SELECT
    datafile_id,
    TRIM(source_instance) AS source_instance,
    date_collecte,
    date_collecte2,
    TRIM(instance_name) AS instance_name,
    TRIM(physical_netbios_name) AS physical_netbios_name,
    TRIM(database_name) AS database_name,
    TRIM(logical_data_name) AS logical_data_name,
    TRIM(file_location) AS file_location,
    total_filesize_mb,
    TRIM(file_type) AS file_type,
    TRIM(filegroup_name) AS filegroup_name,
    inserted_at
FROM {{ source('raw_data', 'raw_datafiles') }}
WHERE datafile_id IS NOT NULL
AND database_name IS NOT NULL
AND TRIM(database_name) <> ''