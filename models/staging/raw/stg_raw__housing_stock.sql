with 

source as (

    select * from {{ source('raw', 'housing_stock') }}

),

renamed as (

    select
        municipality_code,
        year,
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
