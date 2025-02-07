MODEL (
  name gold.episode_event,
  kind FULL,
  cron '@monthly',
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb')
);

SELECT
  ee.episode_id::INT,
  ee.event_id::INT,
  ee.episode_event_field_concept_id::INT
FROM silver.episode_event AS ee
