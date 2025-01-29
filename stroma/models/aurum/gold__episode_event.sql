MODEL (
  name gold.episode_event,
  kind FULL,
  cron '@monthly',
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb'),
  description 'Table capturing events associated with each episode in the OMOP CDM',
  column_descriptions (
    episode_id = 'Unique identifier for each episode',
    event_id = 'Unique identifier for each event within an episode',
    episode_event_field_concept_id = 'Concept ID representing the type of field in the episode event'
  )
);

SELECT
  ee.episode_id,
  ee.event_id,
  ee.episode_event_field_concept_id
FROM silver.episode_event AS ee
