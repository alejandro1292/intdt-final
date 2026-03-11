-- Staging: Procesamiento de modelos LLM y limpieza de datos numéricos
with raw_source as (
    select * from {{ source('airbyte', 'llm') }}
),

cleaned as (
    select
        _id as llm_id,
        model,
        comapany as company, -- Nota: El origen tiene un typo 'comapany'
        arch as architecture,
        -- Conversión de parámetros: Intentamos limpiar si hay texto, aunque el ejemplo muestra números
        case 
            when regexp_replace(parameters, '[^0-9.]', '', 'g') = '' then null
            else cast(regexp_replace(parameters, '[^0-9.]', '', 'g') as double)
        end as parameters_count,
        
        -- Conversión de tokens: Manejo de 'TBA' y valores científicos o decimales
        case 
            when tokens ~ '^[0-9.]+$' and tokens != '' then cast(tokens as double)
            else null 
        end as tokens_count,
        
        -- Ratio: El formato es '20:01', extraemos el primer componente antes de los ':'
        case 
            when split_part(ratio, ':', 1) ~ '^[0-9.]+$' then cast(split_part(ratio, ':', 1) as double)
            else null
        end as ratio_val,
        
        -- Alscore: Manejo de 'TBA'
        case 
            when alscore ~ '^[0-9.]+$' and alscore != '' then cast(alscore as double)
            else null 
        end as ai_score_llm,
        
        training_dataset,
        release_date,
        notes,
        playground as url,
        _airbyte_extracted_at as ingested_at
    from raw_source
)

select * from cleaned