MODEL (
  name silver.observation,
  kind FULL,
  cron '@monthly',
  grain observation_id,
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb'),
  description 'The observation table captures clinical observations made about a person, including measurements, assessments, and other relevant clinical findings.',
  column_descriptions (
      observation_id = 'Unique identifier for each observation',
      person_id = 'Identifier for the person associated with the observation',
      observation_concept_id = 'Concept ID representing the observation',
      observation_date = 'Date when the observation was recorded',
      observation_datetime = 'Date and time when the observation was recorded',
      observation_type_concept_id = 'Concept ID representing the type of observation',
      value_as_number = 'Numeric value of the observation',
      value_as_string = 'String value of the observation',
      value_as_concept_id = 'Concept ID for the value of the observation',
      qualifier_concept_id = 'Concept ID for the qualifier of the observation',
      unit_concept_id = 'Concept ID for the unit of measurement',
      provider_id = 'Identifier for the provider who recorded the observation',
      visit_occurrence_id = 'Identifier for the visit occurrence associated with the observation',
      visit_detail_id = 'Identifier for the visit detail associated with the observation',
      observation_source_value = 'Source value representing the observation',
      observation_source_concept_id = 'Source concept ID for the observation',
      unit_source_value = 'Source value for the unit of measurement',
      qualifier_source_value = 'Source value for the qualifier of the observation',
      value_source_value = 'Source value for the observation value',
      observation_event_id = 'Identifier for the event associated with the observation',
      obs_event_field_concept_id = 'Concept ID for the field of the event associated with the observation'
  )
);

SELECT
  o.observation_id::BIGINT,
  o.person_id::BIGINT,
  o.observation_concept_id::BIGINT,
  o.observation_date::DATE,
  o.observation_datetime::TIMESTAMP,
  o.observation_type_concept_id::BIGINT,
  o.value_as_number::REAL,
  o.value_as_string::TEXT,
  o.value_as_concept_id::BIGINT,
  o.qualifier_concept_id::BIGINT,
  o.unit_concept_id::BIGINT,
  o.provider_id::BIGINT,
  o.visit_occurrence_id::BIGINT,
  o.visit_detail_id::BIGINT,
  o.observation_source_value::TEXT,
  o.observation_source_concept_id::BIGINT,
  o.unit_source_value::TEXT,
  o.qualifier_source_value::TEXT,
  o.value_source_value::TEXT,
  o.observation_event_id::TEXT,
  o.obs_event_field_concept_id::TEXT
/* o.unique_key, */ /* o.datasource, */ /* o.updated_at */
FROM bronze.observation AS o 

