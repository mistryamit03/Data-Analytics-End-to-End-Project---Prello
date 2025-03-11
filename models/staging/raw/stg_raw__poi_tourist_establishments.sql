with 

source as (

    select * from {{ source('raw', 'poi_tourist_establishments') }}

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
