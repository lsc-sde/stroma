
MODEL (
  name @temp_schema.achilles__rawData_806,
  kind FULL,
  cron '@monthly',
  blueprints (
    (schema_achilles := silver_achilles, src_schema := silver, @temp_schema := z_tmp_silver_achilles),
    (schema_achilles := gold_achilles,  src_schema := gold, @temp_schema := z_tmp_gold_achilles)
)
, dialect 'databricks'
);

-- 806	Distribution of age by observation_concept_id
select
  o.observation_concept_id as subject_id,
  p.gender_concept_id,
  o.observation_start_year - p.year_of_birth::FLOAT as count_value
from
  @src_schema.person as p
inner join (
  select
    o.person_id,
    o.observation_concept_id,
    MIN(YEAR(o.observation_date)) as observation_start_year
  from
    @src_schema.observation as o
  inner join
    @src_schema.observation_period as op
    on
      o.person_id = op.person_id
      and
      o.observation_date >= op.observation_period_start_date
      and
      o.observation_date <= op.observation_period_end_date
  group by
    o.person_id,
    o.observation_concept_id
) as o
  on
    p.person_id = o.person_id
