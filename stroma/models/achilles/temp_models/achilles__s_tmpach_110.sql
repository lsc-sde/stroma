
MODEL (
  name @temp_schema.achilles__s_tmpach_110,
  kind FULL,
  cron '@monthly',
  blueprints (
    (schema_achilles := silver_achilles, src_schema := silver, @temp_schema := z_tmp_silver_achilles),
    (schema_achilles := gold_achilles,  src_schema := gold, @temp_schema := z_tmp_gold_achilles)
)
, dialect 'databricks'
);

-- 110	Number of persons with continuous observation in each month
-- Note: using temp table instead of nested query because this gives vastly improved performance in Oracle
select
  110 as analysis_id,
  CAST(t1.obs_month as VARCHAR(255)) as stratum_1,
  CAST(null as VARCHAR(255)) as stratum_2,
  CAST(null as VARCHAR(255)) as stratum_3,
  CAST(null as VARCHAR(255)) as stratum_4,
  CAST(null as VARCHAR(255)) as stratum_5,
  COUNT(distinct op1.PERSON_ID)::FLOAT as count_value
from
  @src_schema.observation_period as op1
inner join
  (
    select distinct
      YEAR(observation_period_start_date) * 100
      + MONTH(observation_period_start_date) as obs_month,
      MAKE_DATE(
        YEAR(observation_period_start_date),
        MONTH(observation_period_start_date),
        1
      )
      as obs_month_start,
      LAST_DAY(observation_period_start_date) as obs_month_end
    from @src_schema.observation_period
  ) as t1
  on
    op1.observation_period_start_date <= t1.obs_month_start
    and op1.observation_period_end_date >= t1.obs_month_end
group by t1.obs_month
