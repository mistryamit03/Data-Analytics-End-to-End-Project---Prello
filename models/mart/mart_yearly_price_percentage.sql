WITH x AS (
    SELECT
        b.department_code AS department_code,
        b.department_name AS department_name,
        EXTRACT(YEAR FROM a.sales_date) AS sales_year,
        AVG(a.sales_amount) AS avg_sales_amount,
        a.premise_type,
        AVG(a.surface) AS avg_total_m2,
        AVG(a.number_of_principal_rooms) AS avg_nb_of_principal_rooms,
        AVG(a.sales_price_m_2) AS avg_sales_price_m_2
    FROM 
        {{ ref("int_notary_real_estate_sales") }} AS a
    LEFT JOIN
        {{ ref("stg_raw__geographical_referential") }} AS b ON a.municipality_code = b.municipality_code
    WHERE 
        b.department_code IS NOT NULL
    GROUP BY 
        b.department_code, b.department_name, EXTRACT(YEAR FROM a.sales_date), a.premise_type
    ORDER BY 
        b.department_code, EXTRACT(YEAR FROM a.sales_date), a.premise_type
),
YearlyGrowth AS (
    SELECT
        department_code,
        department_name,
        sales_year,
        premise_type,
        avg_sales_amount,
        avg_total_m2,
        avg_nb_of_principal_rooms,
        avg_sales_price_m_2,
        (avg_sales_amount - LAG(avg_sales_amount) OVER (PARTITION BY department_code, premise_type ORDER BY sales_year)) / LAG(avg_sales_amount) OVER (PARTITION BY department_code, premise_type ORDER BY sales_year) AS pct_growth_sales_amount,
        (avg_total_m2 - LAG(avg_total_m2) OVER (PARTITION BY department_code, premise_type ORDER BY sales_year)) / LAG(avg_total_m2) OVER (PARTITION BY department_code, premise_type ORDER BY sales_year) AS pct_growth_total_m2,
        (avg_nb_of_principal_rooms - LAG(avg_nb_of_principal_rooms) OVER (PARTITION BY department_code, premise_type ORDER BY sales_year)) / LAG(avg_nb_of_principal_rooms) OVER (PARTITION BY department_code, premise_type ORDER BY sales_year) AS pct_growth_nb_of_principal_rooms,
        (avg_sales_price_m_2 - LAG(avg_sales_price_m_2) OVER (PARTITION BY department_code, premise_type ORDER BY sales_year)) / LAG(avg_sales_price_m_2) OVER (PARTITION BY department_code, premise_type ORDER BY sales_year) AS pct_growth_sales_price_m_2
    FROM x
)
SELECT 
    department_code,
    department_name,
    sales_year,
    premise_type,
    avg_sales_amount,
    pct_growth_sales_amount,
    avg_total_m2,
    pct_growth_total_m2,
    avg_nb_of_principal_rooms,
    pct_growth_nb_of_principal_rooms,
    avg_sales_price_m_2,
    pct_growth_sales_price_m_2
FROM YearlyGrowth
ORDER BY department_code, sales_year, premise_type