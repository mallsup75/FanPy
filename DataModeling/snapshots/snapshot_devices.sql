  {% snapshot snapshot_devices %}

    {{
        config(
            target_schema='copeland_schema',
            unique_key='deviceid',
            strategy='check',
            check_cols=['device_id', 'devicetype'],
            invalidate_hard_deletes=true
        )
    }}

    SELECT distinct
        deviceid,
        devicetype
        

     FROM {{ ref('stg_copeland__devices') }}

{% endsnapshot %}
