select
  md5(coalesce(district,'') || ':' || coalesce(ward,'') || ':' || coalesce(community_area,'')) as location_id,
  district,
  ward,
  community_area
from {{ ref('stg_crimes') }}
group by 1,2,3,4
