MODEL (
  name gold.device_exposure,
  kind FULL,
  cron '@monthly',
  grain device_exposure_id,
  references (
    person_id,
    device_concept_id AS concept_id,
    device_type_concept_id AS concept_id,
    provider_id AS provider_id,
    visit_occurrence_id,
    visit_detail_id,
    device_source_concept_id AS concept_id,
    unit_concept_id AS concept_id,
    unit_source_concept_id AS concept_id
  ),
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb'),
  description 'This table stores records of device exposures for persons in the OMOP database.',
  column_descriptions (
    person_id = 'Unique identifier for the person.',
    device_exposure_id = 'Unique identifier for each device exposure record.',
    device_concept_id = 'Concept identifier for the device.',
    device_exposure_start_date = 'Start date of the device exposure.',
    device_exposure_start_datetime = 'Exact start date and time of the device exposure.',
    device_exposure_end_date = 'End date of the device exposure.',
    device_exposure_end_datetime = 'Exact end date and time of the device exposure.',
    device_type_concept_id = 'Concept identifier for the type of device exposure.',
    unique_device_id = 'Unique identifier for the device.',
    production_id = 'Identifier for the production batch of the device.',
    quantity = 'Quantity of the device used.',
    provider_id = 'Identifier for the provider associated with the device exposure.',
    visit_occurrence_id = 'Identifier for the visit occurrence associated with the device exposure.',
    visit_detail_id = 'Identifier for the visit detail associated with the device exposure.',
    device_source_value = 'Source value for the device.',
    device_source_concept_id = 'Concept identifier for the source of the device.',
    unit_concept_id = 'Concept identifier for the unit of measurement.',
    unit_source_value = 'Source value for the unit of measurement.',
    unit_source_concept_id = 'Concept identifier for the source of the unit of measurement.'
  )
);

SELECT
  de.device_exposure_id::INT,
  de.person_id::INT,
  de.device_concept_id::INT,
  de.device_exposure_start_date::DATE,
  de.device_exposure_start_datetime::TIMESTAMP,
  de.device_exposure_end_date::DATE,
  de.device_exposure_end_datetime::TIMESTAMP,
  de.device_type_concept_id::INT,
  de.unique_device_id::TEXT,
  de.production_id::TEXT,
  de.quantity::INT,
  de.provider_id::TEXT,
  de.visit_occurrence_id::INT,
  de.visit_detail_id::INT,
  de.device_source_value::TEXT,
  de.device_source_concept_id::INT,
  de.unit_concept_id::INT,
  de.unit_source_value::TEXT,
  de.unit_source_concept_id::INT
FROM stg_gold.stg__device_exposure AS de
