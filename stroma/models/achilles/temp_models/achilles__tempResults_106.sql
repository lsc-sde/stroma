
MODEL (
  name @temp_schema.achilles__tempResults_106,
  kind FULL,
  cron '@monthly',
  blueprints (
    (schema_achilles := silver_achilles, src_schema := silver, @temp_schema := z_tmp_silver_achilles),
    (schema_achilles := gold_achilles,  src_schema := gold, @temp_schema := z_tmp_gold_achilles)
)
, dialect 'databricks'
);

with overallStats (
  gender_concept_id, avg_value, stdev_value, min_value, max_value, total
) as (
  select
    gender_concept_id,
    avg(1.0 * count_value)::FLOAT as avg_value,
    stddev(count_value)::FLOAT as stdev_value,
    min(count_value)::FLOAT as min_value,
    max(count_value)::FLOAT as max_value,
    count(*) as total
  from @temp_schema.achilles__rawData_106
  group by gender_concept_id
),
statsView (gender_concept_id, count_value, total, rn) as (
  select
    gender_concept_id,
    count_value,
    count(*) as total,
    row_number() over (order by count_value) as rn
  from @temp_schema.achilles__rawData_106
  group by gender_concept_id, count_value
),
priorStats (gender_concept_id, count_value, total, accumulated) as (
  select
    s.gender_concept_id,
    s.count_value,
    s.total,
    sum(p.total) as accumulated
  from statsView as s
  inner join
    statsView as p
    on s.gender_concept_id = p.gender_concept_id and p.rn <= s.rn
  group by s.gender_concept_id, s.count_value, s.total, s.rn
)
select
  106 as analysis_id,
  cast(o.gender_concept_id as VARCHAR(255)) as gender_concept_id,
  o.total::FLOAT as count_value,
  o.min_value,
  o.max_value,
  o.avg_value,
  o.stdev_value,
  min(case when p.accumulated >= .50 * o.total then count_value end)
   ::FLOAT as median_value,
  min(case when p.accumulated >= .10 * o.total then count_value end)
   ::FLOAT as p10_value,
  min(case when p.accumulated >= .25 * o.total then count_value end)
   ::FLOAT as p25_value,
  min(case when p.accumulated >= .75 * o.total then count_value end)
   ::FLOAT as p75_value,
  min(case when p.accumulated >= .90 * o.total then count_value end)
   ::FLOAT as p90_value
from priorStats as p
inner join overallStats as o on p.gender_concept_id = o.gender_concept_id
group by
  o.gender_concept_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
