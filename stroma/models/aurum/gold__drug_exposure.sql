MODEL (
  name gold.drug_exposure,
  kind FULL,
  cron '@monthly',
  grain drug_exposure_id,
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb'),
  description 'Records details of drugs a patient has been exposed to, including prescriptions and administrations.'
);

SELECT
  de.drug_exposure_id,
  de.person_id,
  de.drug_concept_id,
  de.drug_exposure_start_date,
  de.drug_exposure_start_datetime,
  de.drug_exposure_end_date,
  de.drug_exposure_end_datetime,
  de.verbatim_end_date,
  de.drug_type_concept_id,
  de.stop_reason,
  de.refills,
  de.quantity,
  de.days_supply,
  de.sig,
  de.route_concept_id,
  de.lot_number,
  de.provider_id,
  de.visit_occurrence_id,
  de.visit_detail_id,
  de.drug_source_value,
  de.drug_source_concept_id,
  de.route_source_value,
  de.dose_unit_source_value
FROM stg_gold.stg__drug_exposure AS de
