MODEL (
  name gold.device_exposure,
  kind FULL,
  cron '@monthly',
  grain device_exposure_id,
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb')
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
