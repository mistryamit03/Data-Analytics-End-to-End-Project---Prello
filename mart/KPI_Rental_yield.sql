WITH rental_prices AS (
  SELECT 
    municipality_code,
    AVG((COALESCE(rental_max_apartment, 0) + COALESCE(rental_min_apartment, 0)) / 2) AS rental_appart,
    AVG((COALESCE(rental_med_house, 0) + COALESCE(rental_max_house, 0) + COALESCE(rental_min_house, 0)) / 3) AS rental_house
  FROM {{ref("stg_raw__real_estate_info_by_municipality")}}  
  GROUP BY municipality_code  
)
SELECT 
  rp.municipality_code,
  rp.rental_appart,
  rp.rental_house,
  ntr.sales_amount,
  (rp.rental_appart * 12 / ntr.sales_amount) * 100 AS yield_rental_appart,
  (rp.rental_house * 12 / ntr.sales_amount) * 100 AS yield_rental_house
FROM rental_prices rp
LEFT JOIN {{ref("stg_raw__notary_real_estate_sales")}} ntr
USING (municipality_code)