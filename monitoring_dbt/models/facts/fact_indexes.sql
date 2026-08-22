SELECT
    db.database_id,
    ix.owner_name,
    ix.table_name,
    ix.index_name,
    ix.index_id,
    ix.filegroup_name,
    ix.index_type,
    ix.no_of_keys,
    ix.index_size,
    ix.used_size,
    ix.free_size,
    ix.rows_count,
    ix.row_mod_ctr,
    ix.original_fill_factor,
    ix.date_collecte,
    ix.inserted_at
FROM {{ ref('stg_indexes') }} ix
INNER JOIN {{ ref('dim_databases') }} db
    ON TRIM(LOWER(ix.database_name)) = TRIM(LOWER(db.database_name))
INNER JOIN {{ ref('dim_instances') }} i
    ON db.instance_id = i.instance_id
    AND TRIM(LOWER(ix.instance_name)) = TRIM(LOWER(i.instance_name))
WHERE ix.database_name IS NOT NULL
AND ix.table_name IS NOT NULL
AND ix.index_name IS NOT NULL
