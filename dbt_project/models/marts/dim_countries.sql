with mapping as (
    select * from {{ ref('stg_country_mapping') }}
)

select
    md5(upper(trim(iso3_code))) as country_key,
    upper(trim(iso3_code)) as country_code,
    country_name
from mapping
