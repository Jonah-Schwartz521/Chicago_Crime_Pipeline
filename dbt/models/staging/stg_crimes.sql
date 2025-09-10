select
  id,
  case_number,
  occurrence_date as occurrence_ts,
  block,
  iucr,
  primary_type,
  description,
  location_desc,
  arrest as is_arrest,
  domestic as is_domestic,
  beat::text as beat,
  district::text as district,
  ward::text as ward,
  community_area::text as community_area,
  latitude,
  longitude,
  updated_on
from raw.crimes
where occurrence_date between '2001-01-01' and now() + interval '1 day'
