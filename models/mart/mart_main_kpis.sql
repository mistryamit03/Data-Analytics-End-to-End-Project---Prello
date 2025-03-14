SELECT
    a.department_code,
    a.department_name,
    a.municipality_code,
    a.rental_appart,
    a.rental_house,
    a.sales_price_m_2,
    a.premise_type,
    a.yield_rental,
    a.BEP_in_years,
    AVG(b.avg_sal_by_dept_code) AS avg_sal_by_dept_code_overtime,
    AVG(b.median_salary) AS median_salary_overtime,
    AVG(b.avg_ratio) AS avg_sal_to_median_salary_overtime,
    AVG(b.year_growth_ratio) AS avg_sal_yoy_growth_ratio,
    AVG(c.total_sales_municip) AS avg_total_sales_per_municip,
    AVG(c.sales_per_citizen) AS avg_sales_per_citizen,
    d.avg_principal_home,
    d.avg_second_home,
    d.avg_vacants_housing,
    d.avg_tot_housing,
    d.population,
    d.ratio_population_to_principal_home,
    d.ratio_population_to_second_home,
    d.ratio_population_to_vacants_housing,
    d.ratio_population_to_tot_housing,
    e.poi,
    e.nb_poi,
    e.avg_importance_per_poi,
    AVG(f.sum_sales_amount) AS avg_sales_amount_over_time,
    AVG(f.pct_growth_sales_amount) AS pct_growth_sales_amount_over_time,
    AVG(f.avg_total_m2) AS avg_total_m2_over_time,
    AVG(f.pct_growth_total_m2) AS pct_growth_total_m2_over_time,
    AVG(f.avg_nb_of_principal_rooms) AS avg_nb_of_principal_rooms_over_time,
    AVG(f.pct_growth_nb_of_principal_rooms) AS pct_growth_nb_of_principal_rooms_over_time,
    AVG(f.avg_sales_price_m_2) AS avg_sales_price_m_2_over_time,
    AVG(f.pct_growth_sales_price_m_2) AS pct_growth_sales_price_m_2_over_time
FROM {{ ref("mart_KPI_Rental_yield") }} AS a
LEFT JOIN {{ ref("mart_avg_salary_vs_france") }} AS b 
    ON a.municipality_code = b.municipality_code
LEFT JOIN {{ ref("mart_sales_volume_per_citizen") }} AS c 
    ON a.municipality_code = c.municipality_code
LEFT JOIN {{ ref("mart_nb_inhab_accomodation") }} AS d 
    ON a.municipality_code = d.municipality_code
LEFT JOIN {{ ref("mart_tourism_poi") }} AS e 
    ON a.municipality_code = e.municipality_code
LEFT JOIN {{ ref("mart_yearly_price_percentage") }} AS f 
    ON a.municipality_code = f.municipality_code
    AND a.premise_type = f.premise_type
GROUP BY
    a.department_code,
    a.department_name,
    a.municipality_code,
    a.rental_appart,
    a.rental_house,
    a.sales_price_m_2,
    a.premise_type,
    a.yield_rental,
    a.bep_in_years,
    d.avg_principal_home,
    d.avg_second_home,
    d.avg_vacants_housing,
    d.avg_tot_housing,
    d.population,
    d.ratio_population_to_principal_home,
    d.ratio_population_to_second_home,
    d.ratio_population_to_vacants_housing,
    d.ratio_population_to_tot_housing,
    e.poi,
    e.nb_poi,
    e.avg_importance_per_poi
ORDER BY a.department_code, a.department_name, a.municipality_code