with 

source as (

    select * from {{ source('raw', 'notary_real_estate_sales') }}

),

renamed as (

    select
        _line,
        sales_date,
        sales_amount,
        street_number,
        street_code,
        street_name,
        nom_commune,
        municipality_code,
        premise_type,
        surface,
        number_of_principal_rooms,
        sales_price_m_2,
        latitude,
        longitude

    from source

)

select * from renamed
