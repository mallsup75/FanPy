{% snapshot snapshot_customer %}

    {{
        config(
            target_schema='copeland_schema',
            unique_key='customer_id',
            strategy='timestamp',
            updated_at='updated_at',
            invalidate_hard_deletes=true
        )
    }}

    SELECT distinct
        customer_id,
        region,
        updated_at
    FROM {{ ref('stg_copeland__transactions') }}

{% endsnapshot %}
