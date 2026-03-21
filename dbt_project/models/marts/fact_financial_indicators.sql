with unemp as (
    select 
        upper(trim(cast(country_code as varchar))) as country_code,
        upper(trim(cast(year as varchar))) as year,
        unemployment_rate,
        loaded_at
    from {{ ref('stg_unemployment') }}
    where country_code is not null 
),
cpi as (
    select 
        upper(trim(cast(country_code as varchar))) as country_code,
        upper(trim(cast(year as varchar))) as year,
        cpi_value,
        loaded_at
    from {{ ref('stg_cpi') }}
    where country_code is not null 
),
joined as (
    select 
        coalesce(u.country_code, c.country_code) as country_code,
        coalesce(u.year, c.year) as year,
        u.unemployment_rate,
        c.cpi_value,
        greatest(coalesce(u.loaded_at, '1900-01-01'), coalesce(c.loaded_at, '1900-01-01')) as loaded_at
    from unemp u
    full outer join cpi c on u.country_code = c.country_code 
        and u.year = c.year
    -- where coalesce(u.country_code, c.country_code) is not null
),

final as (
    select
        md5(coalesce(u.country_code, c.country_code)) as country_key,
        coalesce(u.country_code, c.country_code) as country_code,
        coalesce(u.year, c.year) as year,
        u.unemployment_rate,
        c.cpi_value,
        greatest(coalesce(u.loaded_at, '1900-01-01'), coalesce(c.loaded_at, '1900-01-01')) as loaded_at
    from unemp u
    full outer join cpi c on u.country_code = c.country_code 
        and u.year = c.year
)

select * from final
