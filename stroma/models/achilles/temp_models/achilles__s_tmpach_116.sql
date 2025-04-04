
MODEL (
  name @temp_schema.achilles__s_tmpach_116,
  kind FULL,
  cron '@monthly',
  blueprints (
    (schema_achilles := silver_achilles, src_schema := silver, @temp_schema := z_tmp_silver_achilles),
    (schema_achilles := gold_achilles,  src_schema := gold, @temp_schema := z_tmp_gold_achilles)
)
, dialect 'databricks'
);

with rawData as (
  select
    t1.obs_year as stratum_1,
    p1.gender_concept_id as stratum_2,
    floor((t1.obs_year - p1.year_of_birth) / 10) as stratum_3,
    count(distinct p1.PERSON_ID)::FLOAT as count_value
  from
    @src_schema.person as p1
  inner join
    @src_schema.observation_period as op1
    on p1.person_id = op1.person_id,
    @temp_schema.achilles__temp_dates_116 as t1
  where
    year(op1.OBSERVATION_PERIOD_START_DATE) <= t1.obs_year
    and year(op1.OBSERVATION_PERIOD_END_DATE) >= t1.obs_year
  group by
    t1.obs_year,
    p1.gender_concept_id,
    floor((t1.obs_year - p1.year_of_birth) / 10)
)

select
  116 as analysis_id,
  count_value,
  cast(stratum_1 as VARCHAR(255)) as stratum_1,
  cast(stratum_2 as VARCHAR(255)) as stratum_2,
  cast(stratum_3 as VARCHAR(255)) as stratum_3,
  cast(null as VARCHAR(255)) as stratum_4,
  cast(null as VARCHAR(255)) as stratum_5
from rawData
