
MODEL (
  name @temp_schema.achilles__s_tmpach_117,
  kind FULL,
  cron '@monthly',
  blueprints (
    (schema_achilles := silver_achilles, src_schema := silver, @temp_schema := z_tmp_silver_achilles),
    (schema_achilles := gold_achilles,  src_schema := gold, @temp_schema := z_tmp_gold_achilles)
)
);

-- 117	Number of persons with at least one day of observation in each month


with months_array as (
  select explode(sequence(DATE'1900-01-01', DATE'2099-12-01', INTERVAL 1 MONTH)) as months
),
date_keys as (
  select date_format(months,'yyyyMM') as obs_month from months_array
)

select
  117 as analysis_id,
  cast(t1.obs_month as varchar(255)) as stratum_1,
  cast(null as varchar(255)) as stratum_2,
  cast(null as varchar(255)) as stratum_3,
  cast(null as varchar(255)) as stratum_4,
  cast(null as varchar(255)) as stratum_5,
  coalesce(count(distinct op1.PERSON_ID), 0)::FLOAT as count_value
from date_keys as t1
left join
  (
    select
      t2.obs_month,
      op2.*
    from @src_schema.observation_period as op2, date_keys as t2
    where
      year(op2.observation_period_start_date) * 100
      + month(op2.observation_period_start_date)
      <= t2.obs_month
      and year(op2.observation_period_end_date) * 100
      + month(op2.observation_period_end_date)
      >= t2.obs_month
  ) as op1 on t1.obs_month = op1.obs_month
group by t1.obs_month
having coalesce(count(distinct op1.PERSON_ID), 0) > 0
