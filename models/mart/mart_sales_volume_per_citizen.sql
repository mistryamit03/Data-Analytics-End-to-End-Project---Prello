WITH population_data AS (
    SELECT
        department_code,
        department_name,
        EXTRACT(YEAR FROM year_date) AS sales_year,
        SUM(population) AS population_municip
    FROM {{ ref('stg_raw__population_by_municipality') }} as a
        left join {{ref("stg_raw__geographical_referential")}} as b on a.municipality_code = b.municipality_code
    GROUP BY department_code, department_name, sales_year
),
sales_data AS (
    SELECT
        department_code,
        department_name,
        EXTRACT(YEAR FROM sales_date) AS sales_year,
        SUM(sales_amount) AS total_sales_municip
    FROM {{ ref('int_notary_real_estate_sales') }} as d
    left join {{ref("stg_raw__geographical_referential")}} as c on d.municipality_code = c.municipality_code
    GROUP BY department_code, department_name, sales_year
)
SELECT
    s.department_code,
    s.department_name,
    s.sales_year,
    s.total_sales_municip,
    p.population_municip,
    SAFE_DIVIDE(s.total_sales_municip, p.population_municip) AS sales_per_citizen
FROM sales_data s
LEFT JOIN population_data p
    ON s.department_code = p.department_code
    AND s.sales_year = p.sales_year
where s.department_code is not null
order by s.department_code, s.department_name
