MODEL (
  name silver.measurement,
  kind FULL,
  cron '@monthly',
  grain measurement_id,
  references (
    person_id,
    measurement_concept_id AS concept_id,
    measurement_type_concept_id AS concept_id,
    operator_concept_id AS concept_id,
    value_as_concept_id AS concept_id,
    unit_concept_id AS concept_id,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    measurement_source_concept_id AS concept_id,
    unit_source_concept_id AS concept_id,
    meas_event_field_concept_id AS concept_id
    ),
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb'),
  description 'Table containing clinical measurements and observations for persons in the OMOP CDM',
  column_descriptions (
    measurement_id = 'Unique identifier for each measurement',
    person_id = 'Unique identifier for each person',
    measurement_concept_id = 'Concept ID representing the type of measurement',
    measurement_date = 'Date of the measurement',
    measurement_datetime = 'Date and time of the measurement',
    measurement_time = 'Time of the measurement',
    measurement_type_concept_id = 'Concept ID representing the type of measurement',
    operator_concept_id = 'Concept ID representing the operator used in the measurement',
    value_as_number = 'Numeric value of the measurement',
    value_as_concept_id = 'Concept ID representing the value of the measurement',
    unit_concept_id = 'Concept ID representing the unit of the measurement',
    range_low = 'Lower range of the measurement',
    range_high = 'Upper range of the measurement',
    provider_id = 'Unique identifier for the provider',
    visit_occurrence_id = 'Unique identifier for the visit occurrence',
    visit_detail_id = 'Unique identifier for the visit detail',
    measurement_source_value = 'Source value for the measurement',
    measurement_source_concept_id = 'Concept ID representing the source of the measurement',
    unit_source_value = 'Source value for the unit of the measurement',
    unit_source_concept_id = 'Concept ID representing the source of the unit',
    value_source_value = 'Source value for the measurement value',
    meas_event_field_concept_id = 'Concept ID representing the field in the measurement event',
    measurement_event_id = 'Unique identifier for the measurement event'
  )
);

SELECT
  m.measurement_id::BIGINT,
  m.person_id::BIGINT,
  m.measurement_concept_id::BIGINT,
  m.measurement_date::DATE,
  m.measurement_datetime::TIMESTAMP,
  m.measurement_time::TIME,
  m.measurement_type_concept_id::BIGINT,
  m.operator_concept_id::BIGINT,
  m.value_as_number::REAL,
  m.value_as_concept_id::BIGINT,
  m.unit_concept_id::BIGINT,
  m.range_low::REAL,
  m.range_high::REAL,
  m.provider_id::BIGINT,
  m.visit_occurrence_id::BIGINT,
  m.visit_detail_id::BIGINT,
  m.measurement_source_value::TEXT,
  m.measurement_source_concept_id::BIGINT,
  m.unit_source_value::TEXT,
  m.unit_source_concept_id::BIGINT,
  m.value_source_value::TEXT,
  m.meas_event_field_concept_id::BIGINT,
  m.measurement_event_id::TEXT
/* m.unique_key::TEXT, */ /* m.datasource::TEXT, */ /* m.updated_at::DATETIME */
FROM bronze.measurement AS m /* WHERE */ /*   m.measurement_datetime BETWEEN @start_ds AND @end_ds */
