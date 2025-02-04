MODEL (
  name gold.episode,
  kind FULL,
  cron '@monthly',
  grain episode_id,
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
  e.episode_id,
  e.person_id,
  e.episode_concept_id,
  e.episode_start_date,
  e.episode_start_datetime,
  e.episode_end_date,
  e.episode_end_datetime,
  e.episode_parent_id,
  e.episode_number,
  e.episode_object_concept_id,
  e.episode_type_concept_id,
  e.episode_source_value,
  e.episode_source_concept_id
FROM silver.episode AS e
