MODEL (
  name silver.observation_period,
  kind VIEW,
  cron '@monthly',
  grain observation_period_id,
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb')
);

SELECT
  row_number() OVER (ORDER BY person_id) AS observation_period_id,
  person_id,
  observation_period_start_date,
  observation_period_end_date,
  32817 AS period_type_concept_id
FROM bronze.observation_period
