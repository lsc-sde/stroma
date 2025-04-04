
MODEL (
  name @temp_schema.achilles__tempResults_716,
  kind FULL,
  cron '@monthly',
  blueprints (
    (schema_achilles := silver_achilles, src_schema := silver, @temp_schema := z_tmp_silver_achilles),
    (schema_achilles := gold_achilles,  src_schema := gold, @temp_schema := z_tmp_gold_achilles)
)
, dialect 'databricks'
);

-- 716	Distribution of refills by drug_concept_id
with rawData (stratum_id, count_value) as (
  select
    de.drug_concept_id as stratum_id,
    de.refills::FLOAT as count_value
  from
    @src_schema.drug_exposure as de
  inner join
    @src_schema.observation_period as op
    on
      de.person_id = op.person_id
      and
      de.drug_exposure_start_date >= op.observation_period_start_date
      and
      de.drug_exposure_start_date <= op.observation_period_end_date
  where
    de.refills is not NULL
),

overallStats (
  stratum_id, avg_value, stdev_value, min_value, max_value, total
) as (
  select
    stratum_id,
    AVG(1.0 * count_value)::FLOAT as avg_value,
    STDDEV(count_value)::FLOAT as stdev_value,
    MIN(count_value)::FLOAT as min_value,
    MAX(count_value)::FLOAT as max_value,
    COUNT(*) as total
  from rawData
  group by stratum_id
),

statsView (stratum_id, count_value, total, rn) as (
  select
    stratum_id,
    count_value,
    COUNT(*) as total,
    ROW_NUMBER() over (partition by stratum_id order by count_value) as rn
  from rawData
  group by stratum_id, count_value
),

priorStats (stratum_id, count_value, total, accumulated) as (
  select
    s.stratum_id,
    s.count_value,
    s.total,
    SUM(p.total) as accumulated
  from statsView as s
  inner join statsView as p on s.stratum_id = p.stratum_id and s.rn >= p.rn
  group by s.stratum_id, s.count_value, s.total, s.rn
)

select
  716 as analysis_id,
  o.total::FLOAT as count_value,
  o.min_value,
  o.max_value,
  o.avg_value,
  o.stdev_value,
  CAST(o.stratum_id as VARCHAR(255)) as stratum_id,
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
from priorStats as p
inner join overallStats as o on p.stratum_id = o.stratum_id
group by
  o.stratum_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
