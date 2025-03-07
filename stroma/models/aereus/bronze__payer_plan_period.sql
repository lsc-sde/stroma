MODEL (
  name bronze.payer_plan_period,
  kind FULL,
  cron '@monthly',
  grain payer_plan_period_id,
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb')
);

SELECT
  payer_plan_period_id,
  person_id,
  payer_plan_period_start_date,
  payer_plan_period_end_date,
  payer_concept_id,
  payer_source_value,
  payer_source_concept_id,
  plan_concept_id,
  plan_source_value,
  plan_source_concept_id,
  sponsor_concept_id,
  sponsor_source_value,
  sponsor_source_concept_id,
  family_source_value,
  stop_reason_concept_id,
  stop_reason_source_value,
  stop_reason_source_concept_id
FROM @catalog_src.@base.payer_plan_period
