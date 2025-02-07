MODEL (
  name gold.observation,
  kind FULL,
  cron '@monthly',
  grain observation_id,
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb')
);

SELECT
  o.observation_id::INT,
  o.person_id::INT,
  o.observation_concept_id::INT,
  o.observation_date::DATE,
  o.observation_datetime::TIMESTAMP,
  o.observation_type_concept_id::INT,
  o.value_as_number::REAL,
  o.value_as_string::TEXT,
  o.value_as_concept_id::INT,
  o.qualifier_concept_id::INT,
  o.unit_concept_id::INT,
  o.provider_id::INT,
  o.visit_occurrence_id::INT,
  o.visit_detail_id::INT,
  o.observation_source_value::TEXT,
  o.observation_source_concept_id::INT,
  o.unit_source_value::TEXT,
  o.qualifier_source_value::TEXT,
  o.value_source_value::TEXT,
  o.observation_event_id::INT,
  o.obs_event_field_concept_id::INT
/* o.unique_key, */ /* o.datasource, */ /* o.updated_at */
FROM stg_gold.stg__observation AS o
