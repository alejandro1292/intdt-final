-- En Airbyte Cloud a MotherDuck, los datos ya vienen "normalizados" por defecto 
-- por lo que no vienen dentro de un campo JSON '_airbyte_data'
with raw_source as (
    select * from {{ source('airbyte', 'data') }}
),

final as (
    select
        _airbyte_raw_id as airbyte_id,
        _airbyte_extracted_at as ingested_at,
        -- Campos extraídos directamente de las columnas confirmadas en MotherDuck
        cast(_id as string) as record_id,
        cast(job_id as integer) as job_id,
        cast(job_role as string) as job_role,
        cast(industry as string) as industry,
        cast(country as string) as country,
        cast("year" as integer) as execution_year,
        cast(automation_risk_percent as numeric) as automation_risk_pct,
        cast(ai_replacement_score as numeric) as ai_replacement_score,
        cast(automation_risk_category as string) as risk_category,
        cast(salary_before_usd as numeric) as salary_before_usd,
        cast(salary_after_usd as numeric) as salary_after_usd
    from raw_source
)

select * from final
