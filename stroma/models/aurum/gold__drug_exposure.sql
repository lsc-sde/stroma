MODEL (
  name gold.drug_exposure,
  kind FULL,
  cron '@monthly',
  grain drug_exposure_id,
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb')
);

SELECT
  de.drug_exposure_id::INT,
  de.person_id::INT,
  de.drug_concept_id::INT,
  de.drug_exposure_start_date::DATE,
  de.drug_exposure_start_datetime::TIMESTAMP,
  de.drug_exposure_end_date::DATE,
  de.drug_exposure_end_datetime::TIMESTAMP,
  de.verbatim_end_date::DATE,
  de.drug_type_concept_id::INT,
  de.stop_reason::TEXT,
  de.refills::INT,
  de.quantity::REAL,
  de.days_supply::INT,
  de.sig::TEXT,
  de.route_concept_id::INT,
  de.lot_number::TEXT,
  de.provider_id::INT,
  de.visit_occurrence_id::INT,
  de.visit_detail_id::INT,
  de.drug_source_value::TEXT,
  de.drug_source_concept_id::INT,
  de.route_source_value::TEXT,
  de.dose_unit_source_value::TEXT
FROM stg_gold.stg__drug_exposure AS de
