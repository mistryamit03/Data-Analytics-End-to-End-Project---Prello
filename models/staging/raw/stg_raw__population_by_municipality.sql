with 

source as (

    select * from {{ source('raw', 'population_by_municipality') }}

),

renamed as (

    select
        municipality_code,
        PARSE_DATE('%Y', CAST(year AS STRING)) as year_date,
        population,
        country_code

    from source

)

select * from renamed
