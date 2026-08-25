{{ config(
    materialized='table'
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key([
        's.server_id',
        'i.instance_name'
    ]) }} AS instance_id,

    s.server_id,

    i.instance_name

FROM {{ ref('stg_instances') }} i

INNER JOIN {{ ref('dim_servers') }} s
    ON TRIM(LOWER(i.server_name)) = TRIM(LOWER(s.server_name))

WHERE i.instance_name IS NOT NULL
AND TRIM(i.instance_name) <> ''
AND s.server_id IS NOT NULL