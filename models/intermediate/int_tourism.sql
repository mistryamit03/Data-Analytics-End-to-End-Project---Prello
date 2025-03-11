select *
from {{ref('stg_raw__POI_touristic_sites_by_municipality')}}
UNION ALL
select *
from {{ref('stg_raw__poi_tourist_establishments')}}