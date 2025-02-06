MODEL (
  name silver.person,
  kind FULL,
  cron '@monthly',
  grain person_id,
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb'),
  description 'The person table contains demographic details about each individual in the database, serving as the central entity for all clinical data.',
  column_descriptions (
    person_id = 'Unique identifier for each person',
    gender_concept_id = 'Concept ID representing the gender of the person',
    year_of_birth = 'Year the person was born',
    month_of_birth = 'Month the person was born',
    day_of_birth = 'Day the person was born',
    birth_datetime = 'Timestamp of the persons birth',
    race_concept_id = 'Concept ID representing the race of the person',
    ethnicity_concept_id = 'Concept ID representing the ethnicity of the person',
    location_id = 'Identifier for the location associated with the person',
    provider_id = 'Identifier for the primary provider associated with the person',
    care_site_id = 'Identifier for the care site associated with the person',
    person_source_value = 'Source value representing the person',
    gender_source_value = 'Source value representing the gender of the person',
    gender_source_concept_id = 'Source concept ID for the gender of the person',
    race_source_value = 'Source value representing the race of the person',
    race_source_concept_id = 'Source concept ID for the race of the person',
    ethnicity_source_value = 'Source value representing the ethnicity of the person',
    ethnicity_source_concept_id = 'Source concept ID for the ethnicity of the person'
  )
);

/* This is the patient table. */
SELECT
  p.person_id,
  p.gender_concept_id,
  YEAR(p.birth_datetime)::INT as year_of_birth, /* This is the year the patient was born */
  MONTH(p.birth_datetime)::INT as month_of_birth,
  p.day_of_birth::INT,
  p.birth_datetime::TIMESTAMP,
  p.race_concept_id,
  p.ethnicity_concept_id,
  p.location_id,
  p.provider_id,
  p.care_site_id,
  p.person_source_value,
  p.gender_source_value,
  p.gender_source_concept_id,
  p.race_source_value,
  p.race_source_concept_id,
  p.ethnicity_source_value,
  p.ethnicity_source_concept_id
FROM bronze.person AS p
