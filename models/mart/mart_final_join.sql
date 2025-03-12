select 
a.department_code,
a.department_name,
a.appart_rent_per_m2,
a.house_rent_per_m2,
a.sales_price_m_2,
a.premise_type,
a.yield_rental,
a.BEP_in_years,
AVG(b.avg_sal_by_dept_code) as avg_sal_by_dept_code_overtime,
AVG(b.median_salary) as median_salary_overtime,
AVG(b.avg_ratio) as avg_sal_to_median_salary_overtime,
AVG(b.year_growth_ratio) as avg_sal_yoy_growth_ratio,
AVG(c.total_sales_municip) as avg_total_sales_per_municip,
AVG(c.population_municip) as avg_population_municip,
AVG(c.sales_per_citizen) as avg_sales_per_citizen,
d.avg_principal_home,
d.avg_second_home,
d.avg_vacants_housing,
d.avg_tot_housing,
d.avg_population,
d.ratio_population_to_principal_home,
d.ratio_population_to_second_home,
d.ratio_population_to_vacants_housing,
d.ratio_population_to_tot_housing,
e.poi,
e.nb_poi,
e.avg_impportance_per_poi


from {{ref ('mart_KPI_Rental_yield')}} as a 
left join {{ref ('mart_avg_salary_vs_france')}} as b 
on a.department_code = b.department_code
left join  {{ref ('mart_sales_volume_per_citizen')}} as c 
on a.department_code = c.department_code
left join {{ref ('mart_nb_inhab_accomodation')}} as d 
on a.department_code = d.department_code
left join {{ref ('mart_tourism_poi')}} as e 
on a.department_code = e.department_code


group by
a.department_code,
a.department_name,
a.appart_rent_per_m2,
a.house_rent_per_m2,
a.sales_price_m_2,
a.premise_type,
a.yield_rental,
a.BEP_in_years, 
d.avg_principal_home,
d.avg_second_home,
d.avg_vacants_housing,
d.avg_tot_housing,
d.avg_population,
d.ratio_population_to_principal_home,
d.ratio_population_to_second_home,
d.ratio_population_to_vacants_housing,
d.ratio_population_to_tot_housing,
e.poi,
e.nb_poi,
e.avg_impportance_per_poi
ORDER by a.department_code,a.department_name