
MODEL (
  name @temp_schema.achilles__tempResults,
  kind FULL,
  cron '@monthly',
  blueprints (
    (schema_achilles := silver_achilles, src_schema := silver, @temp_schema := z_tmp_silver_achilles),
    (schema_achilles := gold_achilles,  src_schema := gold, @temp_schema := z_tmp_gold_achilles)
)
, dialect 'databricks'
);

-- 103	Distribution of age at first observation period
with rawData (person_id, age_value) as (
  select
    p.person_id,
    MIN(YEAR(observation_period_start_date)) - P.YEAR_OF_BIRTH::FLOAT as age_value
  from @src_schema.person as p
  inner join
    @src_schema.observation_period as op
    on p.person_id = op.person_id
  group by p.person_id, p.year_of_birth
),
overallStats (avg_value, stdev_value, min_value, max_value, total) as (
  select
    AVG(1.0 * age_value)::FLOAT as avg_value,
    stddev(age_value)::FLOAT as stdev_value,
    MIN(age_value)::FLOAT as min_value,
    MAX(age_value)::FLOAT as max_value,
    count(*) as total
  from rawData
),
ageStats (age_value, total, rn) as (
  select
    age_value,
    count(*) as total,
    row_number() over (order by age_value) as rn
  from rawData
  group by age_value
),
ageStatsPrior (age_value, total, accumulated) as (
  select
    s.age_value,
    s.total,
    SUM(p.total) as accumulated
  from ageStats as s
  inner join ageStats as p on p.rn <= s.rn
  group by s.age_value, s.total, s.rn
),
tempResults as (
  select
    103 as analysis_id,
    o.total::FLOAT as count_value,
    o.min_value,
    o.max_value,
    o.avg_value,
    o.stdev_value,
    MIN(case when p.accumulated >= .50 * o.total then age_value end)
     ::FLOAT as median_value,
    MIN(case when p.accumulated >= .10 * o.total then age_value end)
     ::FLOAT as p10_value,
    MIN(case when p.accumulated >= .25 * o.total then age_value end)
     ::FLOAT as p25_value,
    MIN(case when p.accumulated >= .75 * o.total then age_value end)
     ::FLOAT as p75_value,
    MIN(case when p.accumulated >= .90 * o.total then age_value end)
     ::FLOAT as p90_value
  --
  from ageStatsPrior as p
  cross join overallStats as o
  group by o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
)
select
  analysis_id,
  cast(null as VARCHAR(255)) as stratum_1,
  cast(null as VARCHAR(255)) as stratum_2,
  cast(null as VARCHAR(255)) as stratum_3,
  cast(null as VARCHAR(255)) as stratum_4,
  cast(null as VARCHAR(255)) as stratum_5,
  count_value,
  min_value,
  max_value,
  avg_value,
  stdev_value,
  median_value,
  p10_value,
  p25_value,
  p75_value,
  p90_value
from tempResults
