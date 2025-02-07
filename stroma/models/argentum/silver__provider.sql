MODEL (
  name silver.provider,
  kind FULL,
  cron '@monthly',
  grain provider_id,
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb')
);

SELECT
  p.provider_id::INT,
  p.provider_name::TEXT,
  p.npi::TEXT,
  p.dea::TEXT,
  p.specialty_concept_id::INT,
  p.care_site_id::INT,
  p.year_of_birth::INT,
  p.gender_concept_id::INT,
  p.provider_source_value::TEXT,
  p.specialty_source_value::TEXT,
  p.specialty_source_concept_id::INT,
  p.gender_source_value::TEXT,
  p.gender_source_concept_id::INT
FROM bronze.provider AS p
