
MODEL (
  name @temp_schema.achilles__overallStats_1817,
  kind FULL,
  cron '@monthly',
  blueprints (
    (schema_achilles := silver_achilles, src_schema := silver, @temp_schema := z_tmp_silver_achilles),
    (schema_achilles := gold_achilles,  src_schema := gold, @temp_schema := z_tmp_gold_achilles)
)
);

-- 1817	Distribution of high range, by observation_concept_id and unit_concept_id
select
  m.subject_id as stratum1_id,
  m.unit_concept_id as stratum2_id,
  AVG(1.0 * m.count_value)::FLOAT as avg_value,
  STDDEV(m.count_value)::FLOAT as stdev_value,
  MIN(m.count_value)::FLOAT as min_value,
  MAX(m.count_value)::FLOAT as max_value,
  COUNT(*) as total
from (
  select
    measurement_concept_id as subject_id,
    unit_concept_id,
    range_high::FLOAT as count_value
  from
    @src_schema.measurement as m
  inner join
    @src_schema.observation_period as op
    on
      m.person_id = op.person_id
      and
      m.measurement_date >= op.observation_period_start_date
      and
      m.measurement_date <= op.observation_period_end_date
  where
    m.unit_concept_id is not NULL
    and
    m.value_as_number is not NULL
    and
    m.range_low is not NULL
    and
    m.range_high is not NULL
) as m
group by
  m.subject_id,
  m.unit_concept_id
