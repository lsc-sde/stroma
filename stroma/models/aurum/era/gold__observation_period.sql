MODEL (
  name gold.observation_period,
  kind FULL,
  cron '@monthly',
  grain observation_period_id,
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb')
);

SELECT
  row_number() OVER (ORDER BY person_id) AS observation_period_id,
  person_id::BIGINT,
  observation_period_start_date::DATE,
  observation_period_end_date::DATE,
  32817 AS period_type_concept_id
FROM stg_gold.stg__observation_period
