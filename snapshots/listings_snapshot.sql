{% snapshot listings_snapshot %}

    {{
        config(
          database='airbnb',
          schema='snapshots',
          strategy='check',
          check_cols='all',
          unique_key='id'
        )
    }}

    select * from {{ source('raw_airbnb_data', 'listings') }}

{% endsnapshot %}