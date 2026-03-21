{{ config(materialized='table') }}

with cpi_history as (
    select 
        country_name,
        country_code,
        year,
        cpi_value as cpi_actual,
        -- Obtenemos el valor del año anterior usando LAG sobre la partición del país
        lag(cpi_value) over (partition by country_code order by year) as cpi_anterior
    from {{ ref('stg_cpi') }}
),

calculado as (
    select 
        country_name,
        country_code,
        year,
        cpi_actual,
        cpi_anterior,
        -- Aplicamos la fórmula de inflación: ((Final - Inicial) / Inicial) * 100
        case 
            when cpi_anterior is not null and cpi_anterior > 0 
            then round(((cpi_actual - cpi_anterior) / cpi_anterior) * 100, 2)
            else null 
        end as inflacion_anual_pct
    from cpi_history
)

select * 
from calculado
-- Filtramos los últimos 10 años para la visualización final, pero el CTE mantuvo el histórico para el LAG
where cast(year as integer) >= (extract(year from current_date) - 10)
order by country_code, year desc
