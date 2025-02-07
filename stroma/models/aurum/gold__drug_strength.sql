MODEL (
  name gold.drug_strength,
  kind FULL,
  cron '@monthly',
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb')
);

SELECT
  ds.drug_concept_id::INT,
  ds.ingredient_concept_id::INT,
  ds.amount_value::REAL,
  ds.amount_unit_concept_id::INT,
  ds.numerator_value::REAL,
  ds.numerator_unit_concept_id::INT,
  ds.denominator_value::REAL,
  ds.denominator_unit_concept_id::INT,
  ds.box_size::INT,
  ds.valid_start_date::DATE,
  ds.valid_end_date::DATE,
  ds.invalid_reason::TEXT
FROM silver.drug_strength AS ds
