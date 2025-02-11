MODEL (
  name bronze.measurement,
  kind FULL,
  cron '@monthly',
  grain unique_key,
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb')
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
FROM @catalog_src.@base.measurement AS m /* WHERE */ /*   m.measurement_datetime BETWEEN @start_ds AND @end_ds */
