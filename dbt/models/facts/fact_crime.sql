select
  id as crime_id,
  occurrence_ts,
  md5(coalesce(district,'') || ':' || coalesce(ward,'') || ':' || coalesce(community_area,'')) as location_id,
  md5(coalesce(primary_type,'') || ':' || coalesce(iucr,'')) as offense_id,
  is_arrest,
  is_domestic,
  latitude,
  longitude
from {{ ref('stg_crimes') }}
