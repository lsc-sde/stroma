
MODEL (
  name @temp_schema.achilles__s_tmpach_1818,
  kind FULL,
  cron '@monthly',
  blueprints (
    (schema_achilles := silver_achilles, src_schema := silver, @temp_schema := z_tmp_silver_achilles),
    (schema_achilles := gold_achilles,  src_schema := gold, @temp_schema := z_tmp_gold_achilles)
)
, dialect 'databricks'
);

select
  1818 as analysis_id,
  CAST(measurement_concept_id as VARCHAR(255)) as stratum_1,
  CAST(unit_concept_id as VARCHAR(255)) as stratum_2,
  CAST(stratum_3 as VARCHAR(255)) as stratum_3,
  CAST(NULL as VARCHAR(255)) as stratum_4,
  CAST(NULL as VARCHAR(255)) as stratum_5,
  COUNT(person_id)::FLOAT as count_value
from
  @temp_schema.achilles__rawData_1818
group by
  measurement_concept_id,
  unit_concept_id,
  stratum_3
