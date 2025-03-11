with 

source as (

    select * from {{ source('raw', 'POI_touristic_sites_by_municipality') }}

),

renamed as (

    select
        poi,
        name,
        latitude,
        longitude,
        municipality_code,
        importance,
        name_reprocessed

    from source

)

select * from renamed
