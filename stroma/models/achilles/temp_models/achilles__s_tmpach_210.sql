
MODEL (
  name @temp_schema.achilles__s_tmpach_210,
  kind FULL,
  cron '@monthly',
  blueprints (
    (schema_achilles := silver_achilles, src_schema := silver, @temp_schema := z_tmp_silver_achilles),
    (schema_achilles := gold_achilles,  src_schema := gold, @temp_schema := z_tmp_gold_achilles)
)
, dialect 'databricks'
);

-- 210 Number of visit_occurrence records outside a valid observation period
select
  210 as analysis_id,
  CAST(NULL as VARCHAR(255)) as stratum_1,
  CAST(NULL as VARCHAR(255)) as stratum_2,
  CAST(NULL as VARCHAR(255)) as stratum_3,
  CAST(NULL as VARCHAR(255)) as stratum_4,
  CAST(NULL as VARCHAR(255)) as stratum_5,
  COUNT(*)::FLOAT as count_value
from
  @src_schema.visit_occurrence as vo
left join
  @src_schema.observation_period as op
  on
    vo.person_id = op.person_id
    and
    vo.visit_start_date >= op.observation_period_start_date
    and
    vo.visit_start_date <= op.observation_period_end_date
where
  op.person_id is NULL
