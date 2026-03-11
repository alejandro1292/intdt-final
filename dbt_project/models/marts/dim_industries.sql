with stg as (
    select distinct industry from {{ ref('stg_ai_impact') }}
)

select
    md5(industry) as industry_key,
    industry as industry_name
from stg
