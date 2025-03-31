MODEL (
  name silver.drug_strength,
  kind FULL,
  cron '@monthly',
  references (
    person_id,
    drug_concept_id AS concept_id,
    ingredient_concept_id AS concept_id,
    amount_unit_concept_id AS concept_id,
    numerator_unit_concept_id AS concept_id,
    denominator_unit_concept_id AS concept_id
    ),
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb'), 
  description 'The drug_strength table stores data on the strength and concentration of drug ingredients, including measurement units and validity periods.', 
  column_descriptions (
    drug_concept_id = 'Identifier for the drug concept',
    ingredient_concept_id = 'Identifier for the ingredient concept',
    amount_value = 'Amount of the ingredient in the drug',
    amount_unit_concept_id = 'Identifier for the unit of the amount',
    numerator_value = 'Numerator value for the concentration',
    numerator_unit_concept_id = 'Identifier for the unit of the numerator',
    denominator_value = 'Denominator value for the concentration',
    denominator_unit_concept_id = 'Identifier for the unit of the denominator',
    box_size = 'Size of the box containing the drug',
    valid_start_date = 'Start date of the validity period',
    valid_end_date = 'End date of the validity period',
    invalid_reason = 'Reason for invalidation'
  )
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
FROM bronze.drug_strength AS ds
