{{ config(
    materialized='incremental',
    unique_key='job_key || industry_key || location_key || ingested_at',
    on_schema_change='fail'
) }}

with stg as (
    select * from {{ ref('stg_ai_impact') }}
    {% if is_incremental() %}
    where ingested_at > (select max(ingested_at) from {{ this }})
    {% endif %}
)

select
    -- Foreign Keys integradas vía MD5
    md5(job_role) as job_key,
    md5(industry) as industry_key,
    md5(country) as location_key,
    
    -- Hechos y Métricas
    salary_before,
    salary_after,
    (salary_after - salary_before) as salary_delta,
    ai_score,
    automation_risk,
    skill_growth,
    ingested_at
from stg
