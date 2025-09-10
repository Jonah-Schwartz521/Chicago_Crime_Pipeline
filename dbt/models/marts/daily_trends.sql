with f as (select * from {{ ref('fact_crime') }})
select
  date_trunc('day', occurrence_ts) as day,
  count(*) as crimes,
  avg(case when is_arrest then 1 else 0 end)::float as arrest_rate
from f
group by 1
order by 1
