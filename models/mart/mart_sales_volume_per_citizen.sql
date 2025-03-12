WITH population_data AS (
    SELECT
        municipality_code,
        EXTRACT(YEAR FROM year_date) AS sales_year,
        SUM(population) AS population_municip
    FROM {{ ref('stg_raw__population_by_municipality') }}
    GROUP BY municipality_code, sales_year
),
sales_data AS (
    SELECT
        municipality_code,
        EXTRACT(YEAR FROM sales_date) AS sales_year,
        SUM(sales_amount) AS sales_municip
    FROM {{ ref('int_notary_real_estate_sales') }}
    GROUP BY municipality_code, sales_year
)
SELECT
    s.municipality_code,
    s.sales_year,
    s.sales_municip,
    p.population_municip,
    SAFE_DIVIDE(s.sales_municip, p.population_municip) AS sales_per_citizen
FROM sales_data s
LEFT JOIN population_data p
    ON s.municipality_code = p.municipality_code
    AND s.sales_year = p.sales_year
