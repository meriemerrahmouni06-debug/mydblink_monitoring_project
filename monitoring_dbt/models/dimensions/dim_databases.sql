{{ config(
    materialized='table'
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'i.instance_id',
        'd.database_name'
    ]) }} AS database_id,

    i.instance_id,

    d.database_name

FROM {{ ref('stg_databases') }} d

INNER JOIN {{ ref('dim_instances') }} i
    ON TRIM(LOWER(d.instance_name)) = TRIM(LOWER(i.instance_name))

WHERE d.database_name IS NOT NULL
AND TRIM(d.database_name) <> ''
AND i.instance_id IS NOT NULL