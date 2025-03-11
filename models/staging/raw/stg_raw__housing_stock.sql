with 

source as (

    select * from {{ source('raw', 'housing_stock') }}

),

renamed as (

    select
        municipality_code,
        PARSE_DATE('%Y', CAST(year AS STRING)) as year_date,
        nb_principal_home,
        nb_second_home,
        nb_vacants_housing,
        nb_tot_housing,
        secondary_home_rate,
        principal_home_rate,
        vacants_housing_rate,
        country_code

    from source

)

select * from renamed
