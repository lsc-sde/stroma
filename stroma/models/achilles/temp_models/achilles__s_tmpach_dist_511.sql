
MODEL (
  name @temp_schema.achilles__s_tmpach_dist_511,
  kind FULL,
  cron '@monthly',
  blueprints (
    (schema_achilles := silver_achilles, src_schema := silver, @temp_schema := z_tmp_silver_achilles),
    (schema_achilles := gold_achilles,  src_schema := gold, @temp_schema := z_tmp_gold_achilles)
)
);

-- 511	Distribution of time from death to last condition
SELECT
  511 AS analysis_id,
  CAST(NULL AS VARCHAR(255)) AS stratum_1,
  CAST(NULL AS VARCHAR(255)) AS stratum_2,
  CAST(NULL AS VARCHAR(255)) AS stratum_3,
  CAST(NULL AS VARCHAR(255)) AS stratum_4,
  CAST(NULL AS VARCHAR(255)) AS stratum_5,
  count(count_value)::FLOAT as count_value,
  MIN(count_value)::FLOAT as min_value,
  MAX(count_value)::FLOAT as max_value,
  AVG(1.0 * count_value)::FLOAT as avg_value,
  stddev(count_value)::FLOAT as stdev_value,
  MAX(CASE WHEN p1 <= 0.50 THEN count_value ELSE -9999 END)::FLOAT as median_value,
  MAX(CASE WHEN p1 <= 0.10 THEN count_value ELSE -9999 END)::FLOAT as p10_value,
  MAX(CASE WHEN p1 <= 0.25 THEN count_value ELSE -9999 END)::FLOAT as p25_value,
  MAX(CASE WHEN p1 <= 0.75 THEN count_value ELSE -9999 END)::FLOAT as p75_value,
  MAX(CASE WHEN p1 <= 0.90 THEN count_value ELSE -9999 END)::FLOAT as p90_value
FROM (
  SELECT
    datediff( 'day', co.max_date, d.death_date)::FLOAT as count_value,    1.0
    * (ROW_NUMBER() OVER (ORDER BY  datediff( 'day',  co.max_date,d.death_date)))
    / (count(*) OVER () + 1) AS p1
  FROM
    @src_schema.death AS d
    JOIN (
    SELECT
      co.person_id,
      MAX(co.condition_start_date) AS max_date
    FROM
      @src_schema.condition_occurrence AS co
    INNER JOIN
      @src_schema.observation_period AS op
      ON
        co.person_id = op.person_id
        AND
        co.condition_start_date >= op.observation_period_start_date
        AND
        co.condition_start_date <= op.observation_period_end_date
    GROUP BY
      co.person_id
  ) AS co
    ON d.person_id = co.person_id
) AS t1
