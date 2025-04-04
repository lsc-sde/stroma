
MODEL (
  name @temp_schema.achilles__tempResults_907,
  kind FULL,
  cron '@monthly',
  blueprints (
    (schema_achilles := silver_achilles, src_schema := silver, @temp_schema := z_tmp_silver_achilles),
    (schema_achilles := gold_achilles,  src_schema := gold, @temp_schema := z_tmp_gold_achilles)
)
, dialect 'databricks'
);

-- 907	Distribution of drug era length, by drug_concept_id
with rawData (stratum1_id, count_value) as (
  select
    de.drug_concept_id as stratum1_id,
    datediff( 'day',  de.drug_era_end_date,de.drug_era_start_date)::FLOAT as count_value
  from
    @src_schema.drug_era as de
  inner join
    @src_schema.observation_period as op
    on
      de.person_id = op.person_id
      and
      de.drug_era_start_date >= op.observation_period_start_date
      and
      de.drug_era_start_date <= op.observation_period_end_date
),

overallStats (
  stratum1_id, avg_value, stdev_value, min_value, max_value, total
) as (
  select
    stratum1_id,
    AVG(1.0 * count_value)::FLOAT as avg_value,
    STDDEV(count_value)::FLOAT as stdev_value,
    MIN(count_value)::FLOAT as min_value,
    MAX(count_value)::FLOAT as max_value,
    COUNT(*) as total
  from rawData
  group by stratum1_id
),

statsView (stratum1_id, count_value, total, rn) as (
  select
    stratum1_id,
    count_value,
    COUNT(*) as total,
    ROW_NUMBER() over (partition by stratum1_id order by count_value) as rn
  from rawData
  group by stratum1_id, count_value
),

priorStats (stratum1_id, count_value, total, accumulated) as (
  select
    s.stratum1_id,
    s.count_value,
    s.total,
    SUM(p.total) as accumulated
  from statsView as s
  inner join statsView as p on s.stratum1_id = p.stratum1_id and s.rn >= p.rn
  group by s.stratum1_id, s.count_value, s.total, s.rn
)

select
  907 as analysis_id,
  o.total::FLOAT as count_value,
  o.min_value,
  o.max_value,
  o.avg_value,
  o.stdev_value,
  CAST(p.stratum1_id as VARCHAR(255)) as stratum_1,
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
inner join overallStats as o on p.stratum1_id = o.stratum1_id
group by
  p.stratum1_id, o.total, o.min_value, o.max_value, o.avg_value, o.stdev_value
