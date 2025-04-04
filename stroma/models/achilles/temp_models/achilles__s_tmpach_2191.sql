
MODEL (
  name @temp_schema.achilles__s_tmpach_2191,
  kind FULL,
  cron '@monthly',
  blueprints (
    (schema_achilles := silver_achilles, src_schema := silver, @temp_schema := z_tmp_silver_achilles),
    (schema_achilles := gold_achilles,  src_schema := gold, @temp_schema := z_tmp_gold_achilles)
)
, dialect 'databricks'
);

-- 2191	Number of total persons that have at least x measurements
select
  2191 as analysis_id,
  CAST(d.device_concept_id as VARCHAR(255)) as stratum_1,
  CAST(d.device_count as VARCHAR(255)) as stratum_2,
  CAST(NULL as VARCHAR(255)) as stratum_3,
  CAST(NULL as VARCHAR(255)) as stratum_4,
  CAST(NULL as VARCHAR(255)) as stratum_5,
  SUM(COUNT(d.person_id))
    over (partition by d.device_concept_id order by d.device_count desc)
 ::FLOAT as count_value
from (
  select
    d.device_concept_id,
    d.person_id,
    COUNT(d.device_exposure_id) as device_count
  from
    @src_schema.device_exposure as d
  inner join
    @src_schema.observation_period as op
    on
      d.person_id = op.person_id
      and
      d.device_exposure_start_date >= op.observation_period_start_date
      and
      d.device_exposure_start_date <= op.observation_period_end_date
  group by
    d.person_id,
    d.device_concept_id
) as d
group by
  d.device_concept_id,
  d.device_count
