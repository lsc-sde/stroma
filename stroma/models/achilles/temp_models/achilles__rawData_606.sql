
MODEL (
  name @temp_schema.achilles__rawData_606,
  kind FULL,
  cron '@monthly',
  blueprints (
    (schema_achilles := silver_achilles, src_schema := silver, @temp_schema := z_tmp_silver_achilles),
    (schema_achilles := gold_achilles,  src_schema := gold, @temp_schema := z_tmp_gold_achilles)
)
);

-- 606	Distribution of age by procedure_concept_id
select
  po.procedure_concept_id as subject_id,
  p.gender_concept_id,
  po.procedure_start_year - p.year_of_birth::FLOAT as count_value
from
  @src_schema.person as p
inner join (
  select
    po.person_id,
    po.procedure_concept_id,
    MIN(YEAR(po.procedure_date)) as procedure_start_year
  from
    @src_schema.procedure_occurrence as po
  inner join
    @src_schema.observation_period as op
    on
      po.person_id = op.person_id
      and
      po.procedure_date >= op.observation_period_start_date
      and
      po.procedure_date <= op.observation_period_end_date
  group by
    po.person_id,
    po.procedure_concept_id
) as po
  on
    p.person_id = po.person_id
