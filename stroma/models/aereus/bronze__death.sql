MODEL (
  name bronze.death,
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
  )
);

/* @UNION('all', bronze_lth.death, bronze_uhmb.death) */
SELECT
  *
FROM bronze_lth.death
