{{ config(materialized='table') }}

with projects as (
    select
        upper(trim(cast(country_code as varchar))) as country_code,
        upper(trim(cast(approval_year as varchar))) as approval_year,
        count(project_id) as total_projects_bid,
        sum(total_cost_usd) as total_investment_bid,
        avg(total_cost_usd) as avg_project_cost_bid,
        count(case when status = 'Implementación' then 1 end) as active_projects
    from {{ ref('stg_bid_projects') }}
    where approval_year is not null and length(approval_year) = 4
    group by 1, 2
),

financials as (
    select 
        upper(trim(cast(country_code as varchar))) as country_code,
        upper(trim(cast(year as varchar))) as year,
        unemployment_rate,
        cpi_value
    from {{ ref('fact_financial_indicators') }}
),

countries as (
    select 
        upper(trim(cast(country_code as varchar))) as country_code,
        country_name
    from {{ ref('dim_countries') }}
)

    select
    c.country_name,
    c.country_code,
    f.year,
    f.unemployment_rate,
    f.cpi_value,
    p.total_projects_bid,
    p.total_investment_bid,
    p.avg_project_cost_bid,
    p.active_projects,
    -- KPI de riesgo financiero simplificado
    (coalesce(f.unemployment_rate, 0) * 0.4 + (coalesce(f.cpi_value, 0) / 100) * 0.6) as simple_risk_index
from financials f
join countries c on f.country_code = c.country_code
left join projects p on f.country_code = p.country_code and f.year = p.approval_year

