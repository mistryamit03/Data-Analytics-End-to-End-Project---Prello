with 

source as (

    select * from {{ source('raw', 'average_salary_by_municipality') }}

),

renamed as (

    select
        _line,
        _fivetran_synced,
        country_code,
        year,
        municipality_code,
        avg_net_salary

    from source

)

select * from renamed
