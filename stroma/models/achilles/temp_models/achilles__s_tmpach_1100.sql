
MODEL (
  name @temp_schema.achilles__s_tmpach_1100,
  kind FULL,
  cron '@monthly',
  blueprints (
    (schema_achilles := silver_achilles, src_schema := silver, @temp_schema := z_tmp_silver_achilles),
    (schema_achilles := gold_achilles,  src_schema := gold, @temp_schema := z_tmp_gold_achilles)
)
);

-- 1100	Number of persons by location 3-digit zip
with rawData as (
  select
    LEFT(l1.zip, 3) as stratum_1,
    COUNT(distinct person_id)::FLOAT as count_value
  from @src_schema.person as p1
  inner join @src_schema.location as l1
    on p1.location_id = l1.location_id
  where
    p1.location_id is not NULL
    and l1.zip is not NULL
  group by LEFT(l1.zip, 3)
)

select
  1100 as analysis_id,
  count_value,
  CAST(stratum_1 as VARCHAR(255)) as stratum_1,
  CAST(NULL as VARCHAR(255)) as stratum_2,
  CAST(NULL as VARCHAR(255)) as stratum_3,
  CAST(NULL as VARCHAR(255)) as stratum_4,
  CAST(NULL as VARCHAR(255)) as stratum_5
from rawData
