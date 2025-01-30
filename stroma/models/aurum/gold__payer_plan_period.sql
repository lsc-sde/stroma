MODEL (
  name gold.payer_plan_period,
  kind FULL,
  cron '@monthly',
  grain payer_plan_period_id,
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb'),
  description 'The payer_plan_period table captures details about the periods during which a person is covered by a specific payer and health plan.',
  column_descriptions (
      payer_plan_period_id = 'Unique identifier for each payer plan period',
      person_id = 'Identifier for the person covered by the payer plan',
      payer_plan_period_start_date = 'Start date of the payer plan period',
      payer_plan_period_end_date = 'End date of the payer plan period',
      payer_concept_id = 'Concept ID representing the payer',
      payer_source_value = 'Source value representing the payer',
      payer_source_concept_id = 'Source concept ID for the payer',
      plan_concept_id = 'Concept ID representing the health plan',
      plan_source_value = 'Source value representing the health plan',
      plan_source_concept_id = 'Source concept ID for the health plan',
      sponsor_concept_id = 'Concept ID representing the sponsor',
      sponsor_source_value = 'Source value representing the sponsor',
      sponsor_source_concept_id = 'Source concept ID for the sponsor',
      family_source_value = 'Source value representing the family coverage',
      stop_reason_concept_id = 'Concept ID for the reason the plan stopped',
      stop_reason_source_value = 'Source value for the reason the plan stopped',
      stop_reason_source_concept_id = 'Source concept ID for the reason the plan stopped'
  )
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
FROM silver.payer_plan_period
