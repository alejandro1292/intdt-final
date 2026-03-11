with stg as (
    select distinct country from {{ ref('stg_ai_impact') }}
)

select
    md5(country) as location_key,
    country as country_name
from stg
