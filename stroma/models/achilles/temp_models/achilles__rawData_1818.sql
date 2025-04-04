
MODEL (
  name @temp_schema.achilles__rawData_1818,
  kind FULL,
  cron '@monthly',
  blueprints (
    (schema_achilles := silver_achilles, src_schema := silver, @temp_schema := z_tmp_silver_achilles),
    (schema_achilles := gold_achilles,  src_schema := gold, @temp_schema := z_tmp_gold_achilles)
)
, dialect 'databricks'
);

-- 1818	Number of observation records below/within/above normal range, by observation_concept_id and unit_concept_id
select
  m.person_id,
  m.measurement_concept_id,
  m.unit_concept_id,
  CAST(case
    when m.value_as_number < m.range_low
      then 'Below Range Low'
    when m.value_as_number >= m.range_low and m.value_as_number <= m.range_high
      then 'Within Range'
    when m.value_as_number > m.range_high
      then 'Above Range High'
    else 'Other'
  end as VARCHAR(255)) as stratum_3
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
  m.value_as_number is not NULL
  and
  m.unit_concept_id is not NULL
  and
  m.range_low is not NULL
  and
  m.range_high is not NULL
