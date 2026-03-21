{{ config(materialized='table') }}

with source as (
    select * from {{ source('airbyte', 'desempleo') }}
    -- Simplificamos la limpieza para evitar borrar datos válidos por error
    where isoalpha3 is not null 
      and country is not null
      and trim(cast(isoalpha3 as varchar)) != ''
),

renamed as (
    select
        coalesce(m.country_name, trim(cast(country as varchar))) as country_name,
        coalesce(m.iso3_code, upper(trim(cast(isoalpha3 as varchar)))) as country_code,
        
        -- Lógica de Año corregida para DuckDB
        case 
            when cast(period as varchar) ~ '^[0-9]{4}$' then cast(period as varchar)
            when date like '%/%' then split_part(date, '/', 3) -- Extrae el año de MM/DD/YYYY
            else regexp_extract(cast(period as varchar), '([0-9]{4})', 1) 
        end as year,

        -- Manejo de Timestamp de Airbyte
        case 
            when date like '%/%' then strptime(left(date, 19), '%m/%d/%Y %H:%M:%S')
            else try_cast(date as timestamp)
        end as record_date,

        -- Limpieza de números (maneja comas y puntos)
        try_cast(replace(replace(cast(value as varchar), ',', '.'), ' ', '') as double) as unemployment_rate,
        
        _airbyte_extracted_at as loaded_at
    from source
    left join {{ ref('stg_country_mapping') }} m 
        on upper(trim(cast(isoalpha3 as varchar))) = upper(trim(m.iso3_code))
        or upper(trim(cast(isoalpha3 as varchar))) = upper(trim(m.bid_code))
)

select * from renamed
-- Validamos que el año sea numérico y tenga 4 dígitos antes de filtrar
where try_cast(year as integer) is not null 
  and length(trim(year)) = 4
  and unemployment_rate is not null