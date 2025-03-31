
MODEL (
  name @temp_schema.achilles__tempResults_1313,
  kind FULL,
  cron '@monthly',
  blueprints (
    (schema_achilles := silver_achilles, src_schema := silver, @temp_schema := z_tmp_silver_achilles),
    (schema_achilles := gold_achilles,  src_schema := gold, @temp_schema := z_tmp_gold_achilles)
)
);

-- 1313	Distribution of length of stay by visit_detail_concept_id
-- restrict to visits inside observation period
WITH rawData (stratum_id, count_value) AS (
  SELECT
    vd.visit_detail_concept_id AS stratum_id,
    datediff( 'day',  vd.visit_detail_end_date,vd.visit_detail_start_date)
     ::FLOAT as count_value
  FROM
    @src_schema.visit_detail AS vd
    JOIN
    @src_schema.observation_period AS op
    ON
      vd.person_id = op.person_id
      AND
      vd.visit_detail_start_date >= op.observation_period_start_date
      AND
      vd.visit_detail_start_date <= op.observation_period_end_date
),
overallStats (
  stratum_id, avg_value, stdev_value, min_value, max_value, total
) AS (
  SELECT
    stratum_id,
    AVG(1.0 * count_value)::FLOAT as avg_value,
    stddev(count_value)::FLOAT as stdev_value,
    MIN(count_value)::FLOAT as min_value,
    MAX(count_value)::FLOAT as max_value,
    count(*) AS total
  FROM
    rawData
  GROUP BY
    stratum_id
),
statsView (stratum_id, count_value, total, rn) AS (
  SELECT
    stratum_id,
    count_value,
    count(*) AS total,
    ROW_NUMBER() OVER (ORDER BY count_value) AS rn
  FROM
    rawData
  GROUP BY
    stratum_id,
    count_value
),
priorStats (stratum_id, count_value, total, accumulated) AS (
  SELECT
    s.stratum_id,
    s.count_value,
    s.total,
    SUM(p.total) AS accumulated
  FROM
    statsView AS s
  INNER JOIN
    statsView AS p
    ON
      s.stratum_id = p.stratum_id
      AND
      p.rn <= s.rn
  GROUP BY
    s.stratum_id,
    s.count_value,
    s.total,
    s.rn
)
SELECT
  1313 AS analysis_id,
  o.total::FLOAT as count_value,
  o.min_value,
  o.max_value,
  o.avg_value,
  o.stdev_value,
  CAST(o.stratum_id AS VARCHAR(255)) AS stratum_id,
  MIN(
    CASE
      WHEN p.accumulated >= .50 * o.total THEN count_value ELSE o.max_value
    END
  )::FLOAT as median_value,
  MIN(
    CASE
      WHEN p.accumulated >= .10 * o.total THEN count_value ELSE o.max_value
    END
  )::FLOAT as p10_value,
  MIN(
    CASE
      WHEN p.accumulated >= .25 * o.total THEN count_value ELSE o.max_value
    END
  )::FLOAT as p25_value,
  MIN(
    CASE
      WHEN p.accumulated >= .75 * o.total THEN count_value ELSE o.max_value
    END
  )::FLOAT as p75_value,
  MIN(
    CASE
      WHEN p.accumulated >= .90 * o.total THEN count_value ELSE o.max_value
    END
  )::FLOAT as p90_value
FROM
  priorStats AS p
INNER JOIN
  overallStats AS o
  ON p.stratum_id = o.stratum_id
GROUP BY
  o.stratum_id,
  o.total,
  o.min_value,
  o.max_value,
  o.avg_value,
  o.stdev_value
