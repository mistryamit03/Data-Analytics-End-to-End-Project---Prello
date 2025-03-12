Select * from {{ref("stg_raw__notary_real_estate_sales")}} as a
left join {{ref("int_econmic_by_region")}} as b on a.municipality_code = b.municipality_code
left join {{ref("stg_raw__geographical_referential")}} as c on a.municipality_code = c.municipality_code