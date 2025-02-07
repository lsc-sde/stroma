MODEL (
  name gold.person,
  kind FULL,
  cron '@monthly',
  grain person_id,
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb')
);

/* This is the patient table. */
SELECT
  p.person_id::INT,
  p.gender_concept_id::INT,
  p.year_of_birth::INT, /* This is the year the patient was born */
  p.month_of_birth::INT,
  p.race_concept_id::INT,
  p.ethnicity_concept_id::INT,
  p.location_id::INT,
  p.provider_id::INT,
  p.care_site_id::INT,
  p.person_source_value::TEXT,
  p.gender_source_value::TEXT,
  p.gender_source_concept_id::INT,
  p.race_source_value::TEXT,
  p.race_source_concept_id::INT,
  p.ethnicity_source_value::TEXT,
  p.ethnicity_source_concept_id::INT
FROM stg_gold.stg__person AS p
