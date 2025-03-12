WITH rental_prices AS (
  SELECT 
    municipality_code,
    AVG((COALESCE(rental_max_apartment, 0) + COALESCE(rental_min_apartment, 0)) / 2) AS rental_appart,
    AVG((COALESCE(rental_med_house, 0) + COALESCE(rental_max_house, 0) + COALESCE(rental_min_house, 0)) / 3) AS rental_house
  FROM {{ ref("stg_raw__real_estate_info_by_municipality") }}  
  GROUP BY municipality_code  
),
filtered_sales AS (
  SELECT 
    municipality_code,
    sales_amount,
    premise_type
  FROM {{ ref("int_notary_real_estate_sales") }}
  WHERE premise_type IN ('Maison', 'Appartement')  
)
SELECT 
  rp.municipality_code,
  rp.rental_appart,
  rp.rental_house,
  fs.sales_amount,
  fs.premise_type,
  CASE 
    WHEN fs.premise_type = 'Appartement' THEN (rp.rental_appart * 12 / NULLIF(fs.sales_amount, 0)) * 100 
  END AS yield_rental_appart,
  CASE 
    WHEN fs.premise_type = 'Maison' THEN (rp.rental_house * 12 / NULLIF(fs.sales_amount, 0)) * 100 
  END AS yield_rental_house
FROM rental_prices rp
LEFT JOIN filtered_sales fs
USING (municipality_code)


