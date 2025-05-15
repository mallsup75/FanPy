{% snapshot snapshot_event_company %}

    {{
        config(
            target_schema='copeland_schema',
            unique_key='event_company_id',
            strategy='check',
            check_cols=['eventuhub_id', 'companyid', 'logged'],
            invalidate_hard_deletes=true
        )
    }}

    SELECT distinct
        event_company_id,
        eventuhub_id,
        companyid
        

    FROM {{ ref('stg_event_company') }}

{% endsnapshot %}
