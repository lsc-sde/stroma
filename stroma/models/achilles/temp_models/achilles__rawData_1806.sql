
MODEL (
  name @temp_schema.achilles__rawData_1806,
  kind FULL,
  cron '@monthly',
  blueprints (
    (schema_achilles := silver_achilles, src_schema := silver, @temp_schema := z_tmp_silver_achilles),
    (schema_achilles := gold_achilles,  src_schema := gold, @temp_schema := z_tmp_gold_achilles)
)
);

-- 1806	Distribution of age by measurement_concept_id
select
  o.measurement_concept_id as subject_id,
  p.gender_concept_id,
  o.measurement_start_year - p.year_of_birth::FLOAT as count_value
from
  @src_schema.person as p
inner join (
  select
    m.person_id,
    m.measurement_concept_id,
    MIN(YEAR(m.measurement_date)) as measurement_start_year
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
  group by
    m.person_id,
    m.measurement_concept_id
) as o
  on
    p.person_id = o.person_id
