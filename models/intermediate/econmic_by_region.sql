SELECT
    pbm.municipality_code,
    pbm.year,
    pbm.population AS total_population,
    asm.avg_net_salary,
    ppm.population AS poverty_population
FROM
    {{ source('population_by_municipality') }} pbm 
LEFT JOIN
    {{ source('average_salary_by_municipality') }} asm
        ON pbm.municipality_code = asm.municipality_code
        AND pbm.year = asm.year
LEFT JOIN
    {{ source('poverty_population_by_municipality') }} ppm
        ON pbm.municipality_code = ppm.municipality_code
        AND pbm.year = ppm.year