SELECT
    m.municipality_code,
    m.year_date,
    m.nb_principal_home,
    m.nb_second_home,
    m.nb_vacants_housing,
    m.nb_tot_housing,
    m.secondary_home_rate,
    m.principal_home_rate,
    m.vacants_housing_rate,
    m.country_code,
    SUM(m.nb_second_home) / SUM(m.nb_tot_housing) as short_term_rental_potential,   
FROM {{ref("stg_raw__housing_stock")}} AS m
group by 
    m.municipality_code,
    m.year_date,
    m.nb_principal_home,
    m.nb_second_home,
    m.nb_vacants_housing,
    m.nb_tot_housing,
    m.secondary_home_rate,
    m.principal_home_rate,
    m.vacants_housing_rate,
    m.country_code