MODEL (
  name gold.note,
  kind FULL,
  cron '@monthly',
  grain note_id,
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb')
);

SELECT
  n.note_id::INT,
  n.person_id::INT,
  n.note_date::DATE,
  n.note_datetime::TIMESTAMP,
  n.note_type_concept_id::INT,
  n.note_class_concept_id::INT,
  n.note_title::TEXT,
  n.note_text::TEXT,
  n.encoding_concept_id::INT,
  n.language_concept_id::INT,
  n.provider_id::INT,
  n.visit_occurrence_id::INT,
  n.visit_detail_id::INT,
  n.note_source_value::TEXT,
  n.note_event_id::INT,
  n.note_event_field_concept_id::INT
FROM silver.note AS n
