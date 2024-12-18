MODEL (
  name silver.cost,
  kind FULL,
  cron '@monthly',
  grain cost_id,
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb')
);

SELECT
  cost_id,
  cost_event_id,
  cost_domain_id,
  cost_type_concept_id,
  currency_concept_id,
  total_charge,
  total_cost,
  total_paid,
  paid_by_payer,
  paid_by_patient,
  paid_patient_copay,
  paid_patient_coinsurance,
  paid_patient_deductible,
  paid_by_primary,
  paid_ingredient_cost,
  paid_dispensing_fee,
  payer_plan_period_id,
  amount_allowed,
  revenue_code_concept_id,
  revenue_code_source_value,
  drg_concept_id,
  drg_source_value
FROM bronze.cost
