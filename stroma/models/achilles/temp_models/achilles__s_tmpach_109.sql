
MODEL (
  name @temp_schema.achilles__s_tmpach_109,
  kind FULL,
  cron '@monthly',
  blueprints (
    (schema_achilles := silver_achilles, src_schema := silver, @temp_schema := z_tmp_silver_achilles),
    (schema_achilles := gold_achilles,  src_schema := gold, @temp_schema := z_tmp_gold_achilles)
)
);

-- 109	Number of persons with continuous observation in each year

with century as (
  select '19' as num
  union
  select '20' as num
),

tens as (
  select '0' as num
  union
  select '1' as num
  union
  select '2' as num
  union
  select '3' as num
  union
  select '4' as num
  union
  select '5' as num
  union
  select '6' as num
  union
  select '7' as num
  union
  select '8' as num
  union
  select '9' as num
),

ones as (
  select '0' as num
  union
  select '1' as num
  union
  select '2' as num
  union
  select '3' as num
  union
  select '4' as num
  union
  select '5' as num
  union
  select '6' as num
  union
  select '7' as num
  union
  select '8' as num
  union
  select '9' as num
),

months as (
  select '01' as num
  union
  select '02' as num
  union
  select '03' as num
  union
  select '04' as num
  union
  select '05' as num
  union
  select '06' as num
  union
  select '07' as num
  union
  select '08' as num
  union
  select '09' as num
  union
  select '10' as num
  union
  select '11' as num
  union
  select '12' as num
),

date_keys as (
  select concat(century.num, tens.num, ones.num, months.num) as obs_month
  from century
  cross join tens
  cross join ones
  cross join months
),

-- From date_keys, we just need each year and the first and last day of each year
ymd as (
  select
    cast(left(obs_month, 4) as integer) as obs_year,
    1 as day_start,
    31 as day_end,
    min(cast(right(left(obs_month, 6), 2) as integer)) as month_start,
    max(cast(right(left(obs_month, 6), 2) as integer)) as month_end
  from date_keys
  where right(left(obs_month, 6), 2) in ('01', '12')
  group by left(obs_month, 4)
),

-- This gives us each year and the first and last day of the year
year_ranges as (
  select
    obs_year,
    make_date(obs_year, month_start, day_start) as obs_year_start,
    make_date(obs_year, month_end, day_end) as obs_year_end
  from ymd
  where
    obs_year
    >= (
      select min(year(observation_period_start_date))
      from @src_schema.observation_period
    )
    and obs_year
    <= (
      select max(year(observation_period_start_date))
      from @src_schema.observation_period
    )
)

select
  109 as analysis_id,
  cast(yr.obs_year as varchar(255)) as stratum_1,
  cast(NULL as varchar(255)) as stratum_2,
  cast(NULL as varchar(255)) as stratum_3,
  cast(NULL as varchar(255)) as stratum_4,
  cast(NULL as varchar(255)) as stratum_5,
  count(distinct op.person_id)::FLOAT as count_value
from
  @src_schema.observation_period as op
cross join
  year_ranges as yr
where
  op.observation_period_start_date <= yr.obs_year_start
  and
  op.observation_period_end_date >= yr.obs_year_end
group by
  yr.obs_year
