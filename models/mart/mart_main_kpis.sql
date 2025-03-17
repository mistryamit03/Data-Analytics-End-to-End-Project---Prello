{{ config(
    materialized='table'
) }}


With kpi as (
    select
    department_code,
    department_name,
    municipality_code,
    rental_appart,
    rental_house,
    sales_price_m_2,
    surface,
    premise_type,
    yield_rental,
    BEP_in_years
from {{ref("mart_KPI_Rental_yield")}}
),
avg_sal as (
    select 
    municipality_code,
    AVG(avg_sal_by_dept_code) AS avg_sal_by_dept_code_overtime,
    AVG(median_salary) AS median_salary_overtime,
    AVG(avg_ratio) AS avg_sal_to_median_salary_overtime,
    AVG(year_growth_ratio) AS avg_sal_yoy_growth_ratio
    from{{ref("mart_avg_salary_vs_france")}}
    Group by municipality_code
),
sal_vol as (
    select 
    municipality_code,
    AVG(total_sales_municip) AS avg_total_sales_per_municip,
    AVG(sales_per_citizen) AS avg_sales_per_citizen
    from {{ref("mart_sales_volume_per_citizen")}}
    Group by municipality_code
),
nb_inhab as (
    select
    municipality_code,
    avg_principal_home,
    avg_second_home,
    avg_vacants_housing,
    avg_tot_housing,
    population,
    ratio_population_to_principal_home,
    ratio_population_to_second_home,
    ratio_population_to_vacants_housing,
    ratio_population_to_tot_housing
    from {{ref("mart_nb_inhab_accomodation")}}
),
poi as (
    select
    municipality_code, 
    count(poi) as cnt_poi,
    sum(nb_poi) as sum_nb_poi,
    avg(avg_importance_per_poi) as avg_importance_per_poi
    from {{ref("mart_tourism_poi")}}
    Group by municipality_code
),
yoy_grwth as (
    select
    municipality_code,
    AVG(sum_sales_amount) AS avg_sum_sales_amount_over_time,
    AVG(pct_growth_sales_amount) AS pct_growth_sales_amount_over_time,
    AVG(avg_total_m2) AS avg_total_m2_over_time,
    AVG(pct_growth_total_m2) AS pct_growth_total_m2_over_time,
    AVG(avg_nb_of_principal_rooms) AS avg_nb_of_principal_rooms_over_time,
    AVG(pct_growth_nb_of_principal_rooms) AS pct_growth_nb_of_principal_rooms_over_time,
    AVG(avg_sales_price_m_2) AS avg_sales_price_m_2_over_time,
    AVG(pct_growth_sales_price_m_2) AS pct_growth_sales_price_m_2_over_time
    from {{ref("mart_yearly_price_percentage")}}
    group by municipality_code
)
select
kpi.department_code,
kpi.department_name,
kpi.municipality_code,
kpi.rental_appart,
kpi.rental_house,
kpi.sales_price_m_2,
kpi.surface,
kpi.premise_type,
kpi.yield_rental,
kpi.BEP_in_years,
avg_sal.avg_sal_by_dept_code_overtime,
avg_sal.median_salary_overtime,
avg_sal.avg_sal_to_median_salary_overtime,
avg_sal.avg_sal_yoy_growth_ratio,
sal_vol.avg_total_sales_per_municip,
sal_vol.avg_sales_per_citizen,
nb_inhab.avg_principal_home,
nb_inhab.avg_second_home,
nb_inhab.avg_vacants_housing,
nb_inhab.avg_tot_housing,
nb_inhab.population,
nb_inhab.ratio_population_to_principal_home,
nb_inhab.ratio_population_to_second_home,
nb_inhab.ratio_population_to_vacants_housing,
nb_inhab.ratio_population_to_tot_housing,
poi.cnt_poi,
poi.sum_nb_poi,
poi.avg_importance_per_poi,
yoy_grwth.avg_sum_sales_amount_over_time,
yoy_grwth.pct_growth_sales_amount_over_time,
yoy_grwth.avg_total_m2_over_time,
yoy_grwth.pct_growth_total_m2_over_time,
yoy_grwth.avg_nb_of_principal_rooms_over_time,
yoy_grwth.pct_growth_nb_of_principal_rooms_over_time,
yoy_grwth.avg_sales_price_m_2_over_time,
yoy_grwth.pct_growth_sales_price_m_2_over_time
from kpi
left join avg_sal on kpi.municipality_code = avg_sal.municipality_code
left join sal_vol on kpi.municipality_code = sal_vol.municipality_code
left join nb_inhab on kpi.municipality_code = nb_inhab.municipality_code
left join poi on kpi.municipality_code = poi.municipality_code
left join yoy_grwth on kpi.municipality_code = yoy_grwth.municipality_code 
order by department_code, department_name, municipality_code

