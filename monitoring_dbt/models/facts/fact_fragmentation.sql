SELECT
    db.database_id,
    fr.date_collecte,
    fr.date_collecte2,
    fr.physical_netbios_name,
    fr.full_obj_name,
    fr.index_id,
    fr.index_name,
    fr.index_type_desc,
    fr.index_depth,
    fr.index_level,
    fr.avg_fragmentation,
    fr.fragment_count,
    fr.rank,
    fr.inserted_at
FROM {{ ref('stg_fragmentation') }} fr
INNER JOIN {{ ref('dim_databases') }} db
    ON TRIM(LOWER(fr.database_name)) = TRIM(LOWER(db.database_name))
INNER JOIN {{ ref('dim_instances') }} i
    ON db.instance_id = i.instance_id
    AND TRIM(LOWER(fr.instance_name)) = TRIM(LOWER(i.instance_name))
WHERE fr.database_name IS NOT NULL
AND fr.full_obj_name IS NOT NULL
