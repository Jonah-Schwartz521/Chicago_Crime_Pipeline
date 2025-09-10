with f as (select * from {{ ref('fact_crime') }})
select
  l.ward,
  count(*) as crimes,
  avg(case when f.is_arrest then 1 else 0 end)::float as arrest_rate
from f
join {{ ref('dim_location') }} l on f.location_id = l.location_id
group by 1
order by crimes desc
