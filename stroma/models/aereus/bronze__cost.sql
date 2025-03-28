MODEL (
  name bronze.cost,
  kind FULL,
  cron '@monthly',
  grain cost_id,
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb'),
  description 'This table stores the cost details for medical events recorded in the OMOP clinical event tables',
  column_descriptions (
  cost_id = 'Unique identifier for each cost record.',
  cost_event_id = 'Identifier linking the cost record to the corresponding medical event.',
  cost_domain_id = 'Indicates the domain of the medical event associated with the cost record.',
  cost_type_concept_id = 'Concept identifier for the type of cost.',
  currency_concept_id = 'Concept identifier for the currency used in the cost record.',
  total_charge = 'The total amount charged for the medical event.',
  total_cost = 'The total cost incurred for the medical event.',
  total_paid = 'The total amount paid for the medical event.',
paid_by_payer = 'The amount paid by the payer.',
paid_by_patient = 'The amount paid by the patient.',
paid_patient_copay = 'The copay amount paid by the patient.',
paid_patient_coinsurance = 'The coinsurance amount paid by the patient.',
paid_patient_deductible = 'The deductible amount paid by the patient.',
paid_by_primary = 'The amount paid by the primary payer.',
paid_ingredient_cost = 'The cost of the ingredients for the medical event.',
paid_dispensing_fee = 'The dispensing fee paid for the medical event.',
payer_plan_period_id = 'Identifier for the payer plan period.',
amount_allowed = 'The allowed amount for the medical event.',
revenue_code_concept_id = 'Concept identifier for the revenue code.',
revenue_code_source_value = 'Source value for the revenue code.',
drg_concept_id = 'Concept identifier for the DRG (Diagnosis Related Group).',
drg_source_value = 'Source value for the DRG (Diagnosis Related Group).'
  )
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
FROM @catalog_src.@base.cost
