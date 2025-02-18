MODEL (
  name gold.person,
  kind FULL,
  cron '@monthly',
  grain person_id,
  references (
    gender_concept_id AS concept_id,
    race_concept_id AS concept_id, 
    ethnicity_concept_id AS concept_id,
    location_id,
    provider_id,
    care_site_id,
    gender_source_concept_id AS concept_id,
    race_source_concept_id AS concept_id,
    ethnicity_source_concept_id AS concept_id
    ),
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
  p.person_id::BIGINT,
  p.gender_concept_id::BIGINT,
  p.year_of_birth::BIGINT, /* This is the year the patient was born */
  p.month_of_birth::BIGINT,
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
  p.ethnicity_source_concept_id::BIGINT
FROM stg_gold.stg__person AS p
