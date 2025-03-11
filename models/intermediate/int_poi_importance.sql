select
    t.poi,
    t.municipality_code,
    b.department_code,
    b.department_name,
    sum(t.importance) as sum_importance
from {{ ref("int_tourism") }} as t
left join
    {{ ref("stg_raw__geographical_referential") }} as b
    on t.municipality_code = b.municipality_code
group by t.poi, t.municipality_code, b.department_code, b.department_name
