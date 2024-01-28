{{
    config(
        unique_key='lga_code'
    )
}}

SELECT a.lga_code, b.lga_name, b.suburb_name 
FROM {{ ref('suburb_stg') }} b 
LEFT JOIN {{ ref('lga_stg') }} a 
ON a.lga_name = b.lga_name