{{
    config(
        materialized='incremental',
        unique_key='transaction_id',
        incremental_strategy='merge',
        on_schema_change='sync_all_columns',
        schema='copeland_schema'
        
    )
}}

SELECT
    transaction_id,
    customer_id,
    transaction_date,
    product_id,
    quantity,
    total_amount,
    payment_method,
    transaction_status,
    transaction_source,
    created_at,
    updated_at
FROM {{ ref('stg_copeland__transactions') }}

{% if is_incremental() %}
    WHERE updated_at > (SELECT COALESCE(MAX(updated_at), '1900-01-01') FROM {{ this }})
{% endif %}
