
MODEL (
  name @temp_schema.achilles__tempResults_1303,
  kind FULL,
  cron '@monthly',
  blueprints (
    (schema_achilles := silver_achilles, src_schema := silver, @temp_schema := z_tmp_silver_achilles),
    (schema_achilles := gold_achilles,  src_schema := gold, @temp_schema := z_tmp_gold_achilles)
)
, dialect 'databricks'
);

-- 1303	Number of distinct visit detail concepts per person
with rawData (person_id, count_value) as (
  select
    vd.person_id,
    count(distinct vd.visit_detail_concept_id)::FLOAT as count_value
  from
    @src_schema.visit_detail as vd
    join
    @src_schema.observation_period as op
    on
      vd.person_id = op.person_id
      and
      vd.visit_detail_start_date >= op.observation_period_start_date
      and
      vd.visit_detail_start_date <= op.observation_period_end_date
  group by
    vd.person_id
),
overallStats (avg_value, stdev_value, min_value, max_value, total) as (
  select
    AVG(1.0 * count_value)::FLOAT as avg_value,
    stddev(count_value)::FLOAT as stdev_value,
    MIN(count_value)::FLOAT as min_value,
    MAX(count_value)::FLOAT as max_value,
    count(*) as total
  from
    rawData
),
statsView (count_value, total, rn) as (
  select
    count_value,
    count(*) as total,
    row_number() over (order by count_value) as rn
  from
    rawData
  group by
    count_value
),
priorStats (count_value, total, accumulated) as (
  select
    s.count_value,
    s.total,
    SUM(p.total) as accumulated
  from
    statsView as s
  inner join
    statsView as p
    on p.rn <= s.rn
  group by
    s.count_value,
    s.total,
    s.rn
)
select
  1303 as analysis_id,
  o.total::FLOAT as count_value,
  o.min_value,
  o.max_value,
  o.avg_value,
  o.stdev_value,
  MIN(
    case
      when p.accumulated >= .50 * o.total then count_value else o.max_value
    end
  )::FLOAT as median_value,
  MIN(
    case
      when p.accumulated >= .10 * o.total then count_value else o.max_value
    end
  )::FLOAT as p10_value,
  MIN(
    case
      when p.accumulated >= .25 * o.total then count_value else o.max_value
    end
  )::FLOAT as p25_value,
  MIN(
    case
      when p.accumulated >= .75 * o.total then count_value else o.max_value
    end
  )::FLOAT as p75_value,
  MIN(
    case
      when p.accumulated >= .90 * o.total then count_value else o.max_value
    end
  )::FLOAT as p90_value
from
  priorStats as p
cross join
  overallStats as o
group by
  o.total,
  o.min_value,
  o.max_value,
  o.avg_value,
  o.stdev_value
