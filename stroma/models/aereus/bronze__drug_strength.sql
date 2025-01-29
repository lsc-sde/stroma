MODEL (
  name bronze.drug_strength,
  kind FULL,
  cron '@monthly',
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
  ds.drug_concept_id,
  ds.ingredient_concept_id,
  ds.amount_value,
  ds.amount_unit_concept_id,
  ds.numerator_value,
  ds.numerator_unit_concept_id,
  ds.denominator_value,
  ds.denominator_unit_concept_id,
  ds.box_size,
  ds.valid_start_date,
  ds.valid_end_date,
  ds.invalid_reason
FROM @catalog_src.@base.drug_strength AS ds
