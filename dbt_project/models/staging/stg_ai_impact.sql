-- Staging: Limpieza y tipado inicial de los datos de Airbyte
with raw_source as (
    select * from {{ source('airbyte', 'data') }}
),

renamed as (
    select
        job_role,
        industry,
        country,
        cast(salary_before_usd as double) as salary_before,
        cast(salary_after_usd as double) as salary_after,
        cast(ai_replacement_score as double) as ai_score,
        cast(automation_risk_percent as double) as automation_risk,
        cast(skill_demand_growth_percent as double) as skill_growth,
        _airbyte_extracted_at as ingested_at
    from raw_source
)

select * from renamed
