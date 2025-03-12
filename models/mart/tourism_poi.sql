SELECT
count(poi) as nb_poi,
    name_reprocessed,
    latitude,
    longitude,
    municipality_code,
    importance
FROM {{ ref("int_tourism") }}
GROUP BY 
    name_reprocessed,
    latitude,
    longitude,
    municipality_code,
    importance
