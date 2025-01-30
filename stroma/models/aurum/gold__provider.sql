MODEL (
  name gold.provider,
  kind FULL,
  cron '@monthly',
  grain provider_id,
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb'),
  description 'The provider table contains information about healthcare providers, including their identifiers, specialties, and affiliations.',
  column_descriptions (
    provider_id = 'Unique identifier for each provider',
    provider_name = 'Name of the provider',
    npi = 'National Provider Identifier',
    dea = 'Drug Enforcement Administration number',
    specialty_concept_id = 'Concept ID representing the provider specialty',
    care_site_id = 'Identifier for the care site associated with the provider',
    year_of_birth = 'Year the provider was born',
    gender_concept_id = 'Concept ID representing the provider gender',
    provider_source_value = 'Source value representing the provider',
    specialty_source_value = 'Source value representing the provider specialty',
    specialty_source_concept_id = 'Source concept ID for the provider specialty',
    gender_source_value = 'Source value representing the provider gender',
    gender_source_concept_id = 'Source concept ID for the provider gender'    
  )
);

SELECT
  p.provider_id,
  p.provider_name,
  p.npi,
  p.dea,
  p.specialty_concept_id,
  p.care_site_id,
  p.year_of_birth,
  p.gender_concept_id,
  p.provider_source_value,
  p.specialty_source_value,
  p.specialty_source_concept_id,
  p.gender_source_value,
  p.gender_source_concept_id
FROM silver.provider AS p
