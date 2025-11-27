MODEL (
  name silver.death,
  kind VIEW,
  cron '@monthly',
  grain person_id,
  references (
    person_id,
    death_type_concept_id AS concept_id,
    cause_concept_id AS concept_id,
    cause_source_concept_id AS concept_id
  ),
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
  audits (
    not_null(columns := (person_id, death_date)),
    unique_values(columns := (person_id)),
    event_not_in_future(column := death_date)
  )
);

SELECT
  d.person_id::BIGINT,
  d.death_date::DATE,
  d.death_datetime::TIMESTAMP,
  d.death_type_concept_id::BIGINT,
  d.cause_concept_id::BIGINT,
  d.cause_source_value::TEXT,
  d.cause_source_concept_id::BIGINT
FROM bronze.death AS d
