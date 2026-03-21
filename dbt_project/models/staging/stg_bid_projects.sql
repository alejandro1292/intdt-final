with source as (
    select * from {{ source('airbyte', 'proyectos_bid') }}
    where try_cast(replace(cast(totl_cost_orig as varchar), ',', '.') as double) is not null
    and upper(trim(cast(cntry_cd as varchar))) not in ('NULL', 'NA', '', 'CNTRY_CD')
),

renamed as (
    select
        cast(p.oper_num as varchar) as project_id,
        cast(p.oper_nm as varchar) as project_name,
        -- Extraer el año directamente para agrupaciones consistentes
        case 
            when p.apprvl_dt like '%/%' then split_part(p.apprvl_dt, '/', 3)
            else cast(year(try_cast(p.apprvl_dt as timestamp)) as varchar)
        end as approval_year,
        try_cast(p.apprvl_dt as timestamp) as approval_date,
        try_cast(p.sign_dt as timestamp) as signature_date,
        coalesce(m.iso3_code, upper(trim(cast(p.cntry_cd as varchar)))) as country_code,
        coalesce(m.country_name, cast(p.cntry_nm as varchar)) as country_name,
        cast(p.publc_sts_nm as varchar) as status,
        cast(p.sector_nm as varchar) as sector,
        cast(p.subsector_nm as varchar) as subsector,
        cast(p.opertyp_nm as varchar) as operation_type,
        try_cast(p.orig_apprvd_useq_amnt as double) as amount_approved_usd,
        try_cast(replace(cast(p.totl_cost_orig as varchar), ',', '.') as double) as total_cost_usd,
        try_cast(p.loc_cntrprt as double) as local_counterpart_usd,
        p.envmntl_clssfctn_cd as environmental_category,
        p.sts_cd as status_code,
        p._airbyte_extracted_at as loaded_at
    from source p
    left join {{ ref('stg_country_mapping') }} m 
        on upper(trim(cast(p.cntry_cd as varchar))) = upper(trim(m.bid_code))
)

select * from renamed
