MODEL (
  name silver.cost,
  kind FULL,
  cron '@monthly',
  grain cost_id,
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb')
);

SELECT
  cost_id::INT,
  cost_event_id::INT,
  cost_domain_id::TEXT,
  cost_type_concept_id::INT,
  currency_concept_id::INT,
  total_charge::REAL,
  total_cost::REAL,
  total_paid::REAL,
  paid_by_payer::REAL,
  paid_by_patient::REAL,
  paid_patient_copay::REAL,
  paid_patient_coinsurance::REAL,
  paid_patient_deductible::REAL,
  paid_by_primary::REAL,
  paid_ingredient_cost::REAL,
  paid_dispensing_fee::REAL,
  payer_plan_period_id::INT,
  amount_allowed::REAL,
  revenue_code_concept_id::INT,
  revenue_code_source_value::TEXT,
  drg_concept_id::INT,
  drg_source_value::TEXT
FROM bronze.cost
