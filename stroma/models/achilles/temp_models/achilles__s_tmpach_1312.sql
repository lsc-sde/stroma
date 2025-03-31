
MODEL (
  name @temp_schema.achilles__s_tmpach_1312,
  kind FULL,
  cron '@monthly',
  blueprints (
    (schema_achilles := silver_achilles, src_schema := silver, @temp_schema := z_tmp_silver_achilles),
    (schema_achilles := gold_achilles,  src_schema := gold, @temp_schema := z_tmp_gold_achilles)
)
);

-- 1312	Number of persons with at least one visit detail by calendar year by gender by age decile
with rawData as (
  select
    p.gender_concept_id as stratum_2,
    YEAR(vd.visit_detail_start_date) as stratum_1,
    FLOOR((YEAR(vd.visit_detail_start_date) - p.year_of_birth) / 10)
    as stratum_3,
    COUNT(distinct vd.person_id)::FLOAT as count_value
  from
    @src_schema.person as p
  inner join
    @src_schema.visit_detail as vd
    on
      p.person_id = vd.person_id
  inner join
    @src_schema.observation_period as op
    on
      vd.person_id = op.person_id
      and
      vd.visit_detail_start_date >= op.observation_period_start_date
      and
      vd.visit_detail_start_date <= op.observation_period_end_date
  group by
    YEAR(vd.visit_detail_start_date),
    p.gender_concept_id,
    FLOOR((YEAR(vd.visit_detail_start_date) - p.year_of_birth) / 10)
)

select
  1312 as analysis_id,
  count_value,
  CAST(stratum_1 as VARCHAR(255)) as stratum_1,
  CAST(stratum_2 as VARCHAR(255)) as stratum_2,
  CAST(stratum_3 as VARCHAR(255)) as stratum_3,
  CAST(NULL as VARCHAR(255)) as stratum_4,
  CAST(NULL as VARCHAR(255)) as stratum_5
from
  rawData
