MODEL (
  name silver.episode,
  kind FULL,
  cron '@monthly',
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb'),
  description 'Table containing information about episodes of care in the OMOP CDM',
  column_descriptions 
  (
    episode_id = 'Unique identifier for each episode',
    person_id = 'Unique identifier for each person',
    episode_concept_id = 'Concept ID representing the type of episode',
    episode_start_date = 'Start date of the episode',
    episode_start_datetime = 'Start date and time of the episode',
    episode_end_date = 'End date of the episode',
    episode_end_datetime = 'End date and time of the episode',
    episode_parent_id = 'Identifier for the parent episode, if applicable',
    episode_number = 'Sequential number of the episode',
    episode_object_concept_id = 'Concept ID representing the object of the episode',
    episode_type_concept_id = 'Concept ID representing the type of episode',
    episode_source_value = 'Source value for the episode',
    episode_source_concept_id = 'Concept ID representing the source of the episode'
  )
);

SELECT
  e.episode_id::INT,
  e.person_id::INT,
  e.episode_concept_id::INT,
  e.episode_start_date::DATE,
  e.episode_start_datetime::TIMESTAMP,
  e.episode_end_date::DATE,
  e.episode_end_datetime::TIMESTAMP,
  e.episode_parent_id::INT,
  e.episode_number::INT,
  e.episode_object_concept_id::INT,
  e.episode_type_concept_id::INT,
  e.episode_source_value::TEXT,
  e.episode_source_concept_id::INT
FROM bronze.episode AS e
