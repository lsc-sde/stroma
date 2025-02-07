MODEL (
  name silver.payer_plan_period,
  kind FULL,
  cron '@monthly',
  grain payer_plan_period_id,
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb')
);

SELECT
  payer_plan_period_id::INT,
  person_id::INT,
  payer_plan_period_start_date::DATE,
  payer_plan_period_end_date::DATE,
  payer_concept_id::INT,
  payer_source_value::TEXT,
  payer_source_concept_id::INT,
  plan_concept_id::INT,
  plan_source_value::TEXT,
  plan_source_concept_id::INT,
  sponsor_concept_id::INT,
  sponsor_source_value::TEXT,
  sponsor_source_concept_id::INT,
  family_source_value::TEXT,
  stop_reason_concept_id::INT,
  stop_reason_source_value::TEXT,
  stop_reason_source_concept_id::INT
FROM bronze.payer_plan_period
