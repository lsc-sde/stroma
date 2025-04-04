
MODEL (
  name @temp_schema.achilles__s_tmpach_119,
  kind FULL,
  cron '@monthly',
  blueprints (
    (schema_achilles := silver_achilles, src_schema := silver, @temp_schema := z_tmp_silver_achilles),
    (schema_achilles := gold_achilles,  src_schema := gold, @temp_schema := z_tmp_gold_achilles)
)
, dialect 'databricks'
);

-- 119  Number of observation period records by period_type_concept_id
select
  119 as analysis_id,
  cast(op1.period_type_concept_id as VARCHAR(255)) as stratum_1,
  cast(null as VARCHAR(255)) as stratum_2,
  cast(null as VARCHAR(255)) as stratum_3,
  cast(null as VARCHAR(255)) as stratum_4,
  cast(null as VARCHAR(255)) as stratum_5,
  count(*)::FLOAT as count_value
from
  @src_schema.observation_period as op1
group by op1.period_type_concept_id
