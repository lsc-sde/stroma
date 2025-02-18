MODEL (
  name silver.care_site,
  kind FULL,
  cron '@monthly',
  grain care_site_id,
  references (location_id, place_of_service_concept_id AS concept_id),
  description 'http://omop-erd.surge.sh/omop_cdm/tables/CARE_SITE.html',
  column_descriptions (
    care_site_id = 'A unique identifier for each Care Site.',
    care_site_name = 'A unique identifier for each Care Site.',
    place_of_service_concept_id = 'A foreign key that refers to a Place of Service Concept ID in the Standardized Vocabularies.',
    location_id = 'A foreign key to the geographic Location in the LOCATION table, where the detailed address information is stored.',
    care_site_source_value = 'The identifier for the Care Site in the source data, stored here for reference.',
    place_of_service_source_value = 'The source code for the Place of Service as it appears in the source data, stored here for reference.'
  )
);

SELECT
  cs.care_site_id::BIGINT, /* A unique identifier for each Care Site. */
  cs.care_site_name::TEXT,
  cs.place_of_service_concept_id::BIGINT,
  cs.location_id::BIGINT,
  cs.care_site_source_value::TEXT,
  cs.place_of_service_source_value::TEXT
FROM bronze.care_site AS cs
