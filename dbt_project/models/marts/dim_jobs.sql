with stg as (
    select distinct job_role from {{ ref('stg_ai_impact') }}
)

select
    md5(job_role) as job_key,
    job_role
from stg
