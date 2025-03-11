with 

source as (

    select * from {{ source('raw', 'average_salary_by_municipality') }}

),

renamed as (

    select
        municipality_code,
        avg_net_salary,
        PARSE_DATE('%Y', CAST(year AS STRING)) as year_date,
        country_code

    from source

)

select * from renamed
