MODEL (
  name gold.episode_event,
  kind FULL,
  cron '@monthly',
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb')
);

SELECT
  ee.episode_id,
  ee.event_id,
  ee.episode_event_field_concept_id
FROM silverepisode_event AS ee
