select
    a.municipality_code as municipality_code,
    a.city_name_normalized as city_name,
    a.latitude as latitude,
    a.longitude as longitude,
    a.department_code as department_code,
    a.epci_code as epci_code,
    a.country_code as country_code,
    case when b.intensite_tension_immo is Null then 0 else b.intensite_tension_immo end as intensite_tension_immo, 
    case when b.rental_max_apartment is Null then 0 else b.rental_max_apartment end as rental_max_apartment,
    case when b.rental_med_house is Null then 0 else b.rental_med_house end as rental_med_house,
    case when b.rental_max_house is Null then 0 else b.rental_max_house end as rental_max_house,
    case when b.rental_min_house is Null then 0 else b.rental_min_house end as rental_min_house,
    case when b.rental_med_all is Null then 0 else b.rental_med_all end as rental_med_all,
    case when b.rental_max_all is Null then 0 else b.rental_max_all end as rental_max_all,
    case when b.rental_min_all is Null then 0 else b.rental_min_all end as rental_min_all
from {{ ref("stg_raw__geographical_referential") }} as a
left join
    {{ ref("stg_raw__real_estate_info_by_municipality") }} as b
    on a.municipality_code = b.municipality_code