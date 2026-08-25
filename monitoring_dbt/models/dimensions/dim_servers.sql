{{ config(
    materialized='table'
) }}

SELECT
    {{ dbt_utils.generate_surrogate_key([
        'server_name'
    ]) }} AS server_id,

    server_name

FROM {{ ref('stg_servers') }}

WHERE server_name IS NOT NULL
AND TRIM(server_name) <> ''