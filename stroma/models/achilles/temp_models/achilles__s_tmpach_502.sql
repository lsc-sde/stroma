
MODEL (
  name @temp_schema.achilles__s_tmpach_502,
  kind FULL,
  cron '@monthly',
  blueprints (
    (schema_achilles := silver_achilles, src_schema := silver, @temp_schema := z_tmp_silver_achilles),
    (schema_achilles := gold_achilles,  src_schema := gold, @temp_schema := z_tmp_gold_achilles)
)
);

-- 502	Number of persons by death month
with rawData as (
  select
    YEAR(d.death_date) * 100 + MONTH(d.death_date) as stratum_1,
    COUNT(distinct d.person_id)::FLOAT as count_value
  from
    @src_schema.death as d
  inner join
    @src_schema.observation_period as op
    on
      d.person_id = op.person_id
      and
      d.death_date >= op.observation_period_start_date
      and
      d.death_date <= op.observation_period_end_date
  group by
    YEAR(d.death_date) * 100 + MONTH(d.death_date)
)

select
  502 as analysis_id,
  count_value,
  CAST(stratum_1 as VARCHAR(255)) as stratum_1,
  CAST(NULL as VARCHAR(255)) as stratum_2,
  CAST(NULL as VARCHAR(255)) as stratum_3,
  CAST(NULL as VARCHAR(255)) as stratum_4,
  CAST(NULL as VARCHAR(255)) as stratum_5
from
  rawData
