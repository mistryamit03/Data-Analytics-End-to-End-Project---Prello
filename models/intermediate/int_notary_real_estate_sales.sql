with
    rankedentries as (
        select
            *,
            row_number() over (
                partition by
                    sales_date,
                    cast(sales_amount as string),
                    cast(street_number as string),
                    street_code,
                    street_name,
                    nom_commune,
                    cast(municipality_code as string),
                    premise_type,
                    cast(surface as string),
                    number_of_principal_rooms,
                    cast(sales_price_m_2 as string),
                    cast(latitude as string),
                    cast(longitude as string)
                order by _line
            ) as rank
        from {{ ref("stg_raw__notary_real_estate_sales") }}
    )
select *
from rankedentries
where rank = 1 
order by _line asc