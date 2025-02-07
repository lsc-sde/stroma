MODEL (
  name gold.episode,
  kind FULL,
  cron '@monthly',
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb')
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
FROM silver.episode AS e
