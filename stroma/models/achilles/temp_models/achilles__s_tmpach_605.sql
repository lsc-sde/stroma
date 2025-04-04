
MODEL (
  name @temp_schema.achilles__s_tmpach_605,
  kind FULL,
  cron '@monthly',
  blueprints (
    (schema_achilles := silver_achilles, src_schema := silver, @temp_schema := z_tmp_silver_achilles),
    (schema_achilles := gold_achilles,  src_schema := gold, @temp_schema := z_tmp_gold_achilles)
)
, dialect 'databricks'
);

-- 605	Number of procedure occurrence records, by procedure_concept_id by procedure_type_concept_id
select
  605 as analysis_id,
  CAST(po.procedure_CONCEPT_ID as VARCHAR(255)) as stratum_1,
  CAST(po.procedure_type_concept_id as VARCHAR(255)) as stratum_2,
  CAST(NULL as VARCHAR(255)) as stratum_3,
  CAST(NULL as VARCHAR(255)) as stratum_4,
  CAST(NULL as VARCHAR(255)) as stratum_5,
  COUNT(po.person_id)::FLOAT as count_value
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
  po.procedure_CONCEPT_ID,
  po.procedure_type_concept_id
