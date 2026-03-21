{{ config(materialized='table') }}

with source as (
    select * from {{ source('airbyte', 'cpi_indice') }}
    where try_cast(replace(cast(value as varchar), ',', '.') as double) is not null
    and upper(trim(cast(iso as varchar))) not in ('NULL', 'NA', '', 'COUNTRY_CODE', 'ISO')
    and upper(trim(cast(country_name as varchar))) not in ('COUNTRY_NAME', 'VALOR', 'NOMBRE_PAIS')
),

renamed as (
    select
        coalesce(m.country_name, cast(source.country_name as varchar)) as country_name,
        coalesce(m.iso3_code, upper(trim(cast(iso as varchar)))) as country_code,
        -- Extraer año de period si es posible, o de la columna date (MM/DD/YYYY)
        case 
            when regexp_matches(cast(period as varchar), '^[0-9]{4}$') then cast(period as varchar)
            when date like '%/%' then split_part(date, '/', 3) -- MM/DD/YYYY
            else regexp_replace(cast(period as varchar), '[^0-9]', '', 'g') 
        end as year,
        -- Manejar formato MM/DD/YYYY HH:MM:SS AM/PM de Airbyte
        case 
            when date like '%/%' then strptime(date, '%m/%d/%Y %I:%M:%S %p')
            else cast(date as timestamp)
        end as record_date,
        cast(replace(cast(value as varchar), ',', '.') as double) as cpi_value,
        _airbyte_extracted_at as loaded_at
    from source
    left join {{ ref('stg_country_mapping') }} m 
        on upper(trim(cast(iso as varchar))) = upper(trim(m.bid_code))
        or upper(trim(cast(iso as varchar))) = upper(trim(m.iso3_code))
)

select * from renamed
where year is not null and length(year) = 4
