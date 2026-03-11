-- Mart: Análisis de Correlación entre Impacto Laboral y Capacidades de LLMs
with impact as (
    select 
        industry,
        avg(automation_risk) as avg_industry_risk,
        avg(ai_score) as avg_industry_ai_score,
        avg(skill_growth) as avg_skill_growth
    from {{ ref('stg_ai_impact') }}
    group by 1
),

llm_power as (
    select 
        company,
        avg(parameters_count) as avg_parameters,
        max(ai_score_llm) as max_llm_score,
        count(llm_id) as num_models
    from {{ ref('stg_llm') }}
    group by 1
)

-- Esta query permite cruzar la potencia de los LLMs (por compañía) 
-- con los indicadores de riesgo de automatización de las industrias
select 
    i.*,
    l.company as leading_developer,
    l.max_llm_score,
    l.avg_parameters,
    l.num_models
from impact i
cross join llm_power l
where l.max_llm_score is not null
order by i.avg_industry_risk desc, l.max_llm_score desc