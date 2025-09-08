MODEL (
  name bronze_@{org}.person,
  kind FULL,
  cron '@monthly',
  grain person_id,
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb'),
  blueprints ((
      org := 'lth'
    ), (
      org := 'uhmb'
  ))
);

/* This is the patient table. */
SELECT
  p.person_id::BIGINT,
  p.gender_concept_id::BIGINT,
  p.year_of_birth::INT, /* This is the year the patient was born */
  p.month_of_birth::INT,
  p.day_of_birth::INT,
  p.birth_datetime::TIMESTAMP,
  p.race_concept_id::BIGINT,
  p.ethnicity_concept_id::BIGINT,
  p.location_id::BIGINT,
  p.provider_id::BIGINT,
  p.care_site_id::BIGINT,
  p.person_source_value::TEXT,
  p.gender_source_value::TEXT,
  p.gender_source_concept_id::BIGINT,
  p.race_source_value::TEXT,
  p.race_source_concept_id::BIGINT,
  p.ethnicity_source_value::TEXT,
  p.ethnicity_source_concept_id::BIGINT,
  @org::TEXT AS org
FROM @catalog_src.@{org}_omop.person AS p
