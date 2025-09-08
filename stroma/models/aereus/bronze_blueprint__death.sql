MODEL (
  name bronze_@{org}.death,
  kind FULL,
  cron '@monthly',
  grain person_id,
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb'),
  description 'This table stores records of death details for persons in the OMOP database.',
  column_descriptions (
    person_id = 'Unique identifier for the person.',
    death_date = 'Date of death.',
    death_datetime = 'Exact date and time of death.',
    death_type_concept_id = 'Concept identifier for the type of death record.',
    cause_concept_id = 'Concept identifier for the cause of death.',
    cause_source_value = 'Source value for the cause of death.',
    cause_source_concept_id = 'Concept identifier for the source of the cause of death.'
  ),
  blueprints ((
      org := 'lth'
    ), (
      org := 'uhmb'
  ))
);

SELECT
  d.person_id::BIGINT AS person_id,
  d.death_date::DATE AS death_date,
  d.death_datetime::TIMESTAMP AS death_datetime,
  d.death_type_concept_id::BIGINT AS death_type_concept_id,
  d.cause_concept_id::BIGINT AS cause_concept_id,
  d.cause_source_value::TEXT AS cause_source_value,
  d.cause_source_concept_id::BIGINT AS cause_source_concept_id,
  @org::TEXT AS org
FROM @catalog_src.@{org}_omop.death AS d
