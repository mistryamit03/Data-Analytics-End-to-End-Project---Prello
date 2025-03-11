with 

source as (

    select * from {{ source('raw', 'POI_touristic_sites_by_municipality') }}

),

renamed as (

    select
        case 
        when poi = '1' then 'Unesco' 
        when poi = '2' then 'Monument_historique' 
        else poi end as poi, 
        name,
        latitude,
        longitude,
        municipality_code,
        importance,
        name_reprocessed

    from source

)

select * from renamed
