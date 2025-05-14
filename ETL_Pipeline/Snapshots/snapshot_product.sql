{% snapshot snapshot_product %}

    {{
        config(
            unique_key = 'product_id',
            strategy = 'timestamp',
            updated_at = 'updated_at',
            invalidate_hard_deletes=true
        )
    }}

    select distinct
    product_id
    ,price_per_unit 
    ,purchase_category
    ,updated_at
    from

     {{ ref('stg_copeland__transactions')}}

{% endsnapshot %}
