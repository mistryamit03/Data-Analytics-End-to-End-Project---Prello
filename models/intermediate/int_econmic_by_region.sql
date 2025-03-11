SELECT
    pbm.municipality_code,
    pbm.year,
    pbm.population AS total_population,
    IFNULL(asm.avg_net_salary, 0) AS avg_net_salary,
    ppm.population AS poverty_population
FROM
    {{ ref('stg_raw__population_by_municipality') }} pbm 
LEFT JOIN
    {{ ref('stg_raw__average_salary_by_municipality') }} asm
        ON pbm.municipality_code = asm.municipality_code
        AND pbm.year = asm.year
LEFT JOIN
    {{ ref("stg_raw__poverty_population_by_municipality") }} ppm
        ON pbm.municipality_code = ppm.municipality_code
        AND pbm.year = ppm.year

