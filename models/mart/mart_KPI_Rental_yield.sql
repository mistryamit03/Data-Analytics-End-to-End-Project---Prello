WITH rental_prices AS (
  SELECT
    department_code,
    department_name,
    AVG((COALESCE(rental_max_apartment, 0) + COALESCE(rental_min_apartment, 0)) / 2) AS rental_appart,
    AVG((COALESCE(rental_med_house, 0) + COALESCE(rental_max_house, 0) + COALESCE(rental_min_house, 0)) / 3) AS rental_house
  FROM {{ ref("stg_raw__real_estate_info_by_municipality") }} as a
  LEFT JOIN {{ ref("stg_raw__geographical_referential") }} as b ON a.municipality_code = b.municipality_code
  GROUP BY department_code, department_name
),
filtered_sales AS (
  SELECT
    department_code,
    department_name,
    sales_price_m_2,
    premise_type
  FROM {{ ref("int_notary_real_estate_sales") }} as c
  LEFT JOIN {{ ref("stg_raw__geographical_referential") }} as b ON c.municipality_code = b.municipality_code
  WHERE premise_type IN ('Maison', 'Appartement')
)
SELECT
  rp.department_code,
  rp.department_name,
  rp.rental_appart AS appart_rent_per_m2,
  rp.rental_house AS house_rent_per_m2,
  fs.sales_price_m_2,
  fs.premise_type,
  CASE
    WHEN fs.premise_type = 'Appartement' THEN (rp.rental_appart * 12 / NULLIF(fs.sales_price_m_2, 0))
    WHEN fs.premise_type = 'Maison' THEN (rp.rental_house * 12 / NULLIF(fs.sales_price_m_2, 0))
  END AS yield_rental,
  CASE
    WHEN fs.premise_type = 'Appartement' THEN 1 / (rp.rental_appart * 12 / NULLIF(fs.sales_price_m_2, 0))
    WHEN fs.premise_type = 'Maison' THEN 1 / (rp.rental_house * 12 / NULLIF(fs.sales_price_m_2, 0))
  END AS BEP_in_years
FROM rental_prices rp
LEFT JOIN filtered_sales fs ON rp.department_code = fs.department_code
ORDER BY rp.department_code