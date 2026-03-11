{{ config(materialized='table') }}

with stg as (
    select * from {{ ref('stg_ai_impact') }}
),

llm_metrics as (
    -- Traemos métricas agregadas de LLMs para dar contexto de potencia tecnológica
    select 
        avg(ai_score_llm) as avg_llm_power,
        max(ai_score_llm) as max_llm_power,
        count(distinct company) as total_llm_players
    from {{ ref('stg_llm') }}
)

select
    -- Atributos descriptivos (Dimensiones desnormalizadas)
    s.job_role,
    s.industry,
    s.country,
    
    -- Métricas (Hechos integrados)
    s.salary_before,
    s.salary_after,
    (s.salary_after - s.salary_before) as salary_delta,
    (s.salary_after - s.salary_before) / nullif(s.salary_before, 0) * 100 as salary_growth_pct,
    s.ai_score,
    s.automation_risk,
    s.skill_growth,
    
    -- Contexto de LLMs (Cross Join para análisis global en Metabase)
    l.avg_llm_power,
    l.max_llm_power,
    l.total_llm_players,
    
    -- Clasificaciones interesantes para Metabase
    case 
        when s.automation_risk > 70 then 'High Risk'
        when s.automation_risk > 30 then 'Medium Risk'
        else 'Low Risk'
    end as risk_segment,
    
    case 
        when s.ai_score > 60 and s.skill_growth > 0 then 'Transformation Opportunity'
        when s.ai_score > 60 and s.skill_growth <= 0 then 'High Displacement Risk'
        else 'Stable/Evolving'
    end as ai_narrative,
    
    s.ingested_at
from stg s
cross join llm_metrics l
