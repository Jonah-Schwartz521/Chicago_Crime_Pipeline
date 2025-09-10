select
  md5(coalesce(primary_type,'') || ':' || coalesce(iucr,'')) as offense_id,
  primary_type,
  iucr,
  description
from {{ ref('stg_crimes') }}
group by 1,2,3,4
