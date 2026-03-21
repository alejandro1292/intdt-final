
with mapping_data as (
    select 'AR' as bid_code, 'ARG' as iso3_code, 'Argentina' as country_name union all
    select 'BA', 'BRB', 'Barbados' union all
    select 'BH', 'BHS', 'Bahamas' union all
    select 'BL', 'BLZ', 'Belice' union all
    select 'BO', 'BOL', 'Bolivia' union all
    select 'BR', 'BRA', 'Brasil' union all
    select 'CH', 'CHL', 'Chile' union all
    select 'CO', 'COL', 'Colombia' union all
    select 'CR', 'CRI', 'Costa Rica' union all
    select 'DR', 'DOM', 'República Dominicana' union all
    select 'EC', 'ECU', 'Ecuador' union all
    select 'ES', 'SLV', 'El Salvador' union all
    select 'GU', 'GTM', 'Guatemala' union all
    select 'GY', 'GUY', 'Guyana' union all
    select 'HA', 'HTI', 'Haití' union all
    select 'HO', 'HND', 'Honduras' union all
    select 'JA', 'JAM', 'Jamaica' union all
    select 'ME', 'MEX', 'México' union all
    select 'NI', 'NIC', 'Nicaragua' union all
    select 'PN', 'PAN', 'Panamá' union all
    select 'PR', 'PRY', 'Paraguay' union all
    select 'PE', 'PER', 'Perú' union all
    select 'SU', 'SUR', 'Surinam' union all
    select 'TT', 'TTO', 'Trinidad y Tobago' union all
    select 'UR', 'URY', 'Uruguay' union all
    select 'VE', 'VEN', 'Venezuela'
)
select distinct * from mapping_data
