SELECT 
    geo.department_code,
    geo.department_name,
    AVG(hs.nb_principal_home) AS avg_principal_home,
    AVG(hs.nb_second_home) AS avg_second_home,
    AVG(hs.nb_vacants_housing) AS avg_vacants_housing,
    AVG(hs.nb_tot_housing) AS avg_tot_housing,
    AVG(ppl.population) AS avg_population,


    SAFE_DIVIDE(AVG(ppl.population), AVG(hs.nb_principal_home)) AS ratio_population_to_principal_home,
    SAFE_DIVIDE(AVG(ppl.population), AVG(hs.nb_second_home)) AS ratio_population_to_second_home,
    SAFE_DIVIDE(AVG(ppl.population), AVG(hs.nb_vacants_housing)) AS ratio_population_to_vacants_housing,
    SAFE_DIVIDE(AVG(ppl.population), AVG(hs.nb_tot_housing)) AS ratio_population_to_tot_housing

FROM {{ ref("stg_raw__housing_stock") }} AS hs
LEFT JOIN {{ ref("stg_raw__population_by_municipality") }} AS ppl on hs.municipality_code = ppl.municipality_code
 left join {{ref("stg_raw__geographical_referential") }} as geo on geo.municipality_code = hs.municipality_code
Where geo.department_code is not null
GROUP BY     geo.department_code,
    geo.department_name
order by geo.department_code,
    geo.department_name

