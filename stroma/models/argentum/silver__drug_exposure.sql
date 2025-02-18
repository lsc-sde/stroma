MODEL (
  name silver.drug_exposure,
  kind FULL,
  cron '@monthly',
  grain drug_exposure_id,
  references (
    person_id,
    drug_concept_id AS concept_id,
    drug_type_concept_id AS concept_id,
    route_concept_id AS concept_id,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    drug_source_concept_id AS concept_id
    ),
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb'),
  description 'Records details of drugs a patient has been exposed to, including prescriptions and administrations.',
  column_descriptions (
    drug_exposure_id = 'A system-generated unique identifier for each Drug utilization event.',
    person_id = 'A foreign key identifier to the person who is subjected to the Drug.',
    drug_concept_id = 'A foreign key that refers to a Standard Concept identifier for the Drug concept.',
    drug_exposure_start_date = 'The start date for the current instance of Drug utilization.',
    drug_exposure_start_datetime = 'The start date and time for the current instance of Drug utilization.',
    drug_exposure_end_date = 'The end date for the current instance of Drug utilization.',
    drug_exposure_end_datetime = 'The end date and time for the current instance of Drug utilization.',
    verbatim_end_date = 'This is the end date of the drug exposure as it appears in the source data, if it is given',
    drug_type_concept_id = 'A foreign key to the predefined Concept identifier reflecting the type of Drug Exposure recorded.',
    stop_reason = 'The reason the Drug was stopped, such as regimen completed, changed, or removed.',
    refills = 'The number of refills after the initial prescription.',
    quantity = 'The quantity of Drug as recorded in the original prescription or dispensing record.',
    days_supply = 'The number of days of supply of the Drug.',
    sig = 'The directions for Drug administration as recorded in the prescription or dispensing record.',
    route_concept_id = 'A foreign key to the predefined Concept identifier indicating the route of Drug administration.',
    lot_number = 'The lot number of the Drug as recorded in the original prescription or dispensing record.',
    provider_id = 'A foreign key identifier to the provider who initiated the Drug Exposure.',
    visit_occurrence_id = 'A foreign key to the visit during which the Drug was prescribed or administered.',
    visit_detail_id = 'A foreign key to the visit detail during which the Drug was prescribed or administered.',
    drug_source_value = 'The source code for the Drug as it appears in the source data.',
    drug_source_concept_id = 'A foreign key to a Drug Concept that refers to the code used in the source.',
    route_source_value = 'The source code for the route of Drug administration as it appears in the source data.',
    dose_unit_source_value = 'The source code for the unit of measure for the dose as it appears in the source data.'
  )
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
FROM bronze.drug_exposure AS de
