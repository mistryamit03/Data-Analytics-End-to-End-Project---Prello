SELECT
    pbm.municipality_code,
    pbm.year_date,
    pbm.population AS total_population,
    IFNULL(asm.avg_net_salary, 0) AS avg_net_salary,
    ppm.population AS poverty_population,
    c.department_code,
    c.department_name
FROM
    {{ ref('stg_raw__population_by_municipality') }} pbm 
LEFT JOIN
    {{ ref('stg_raw__average_salary_by_municipality') }} asm
        ON pbm.municipality_code = asm.municipality_code
        AND pbm.year_date = asm.year_date
LEFT JOIN
    {{ ref("stg_raw__poverty_population_by_municipality") }} ppm
        ON pbm.municipality_code = ppm.municipality_code
        AND pbm.year_date = ppm.year_date
Left join {{ref('stg_raw__geographical_referential')}} as c ON pbm.municipality_code = c.municipality_code 
