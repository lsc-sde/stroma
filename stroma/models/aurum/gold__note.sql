MODEL (
  name gold.note,
  kind FULL,
  cron '@monthly',
  grain note_id,
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb'),
  description 'The note table contains unstructured clinical notes recorded during patient care, providing detailed narrative information.',
  column_descriptions (
    note_id = 'Unique identifier for each note',
    person_id = 'Identifier for the person associated with the note',
    note_date = 'Date when the note was recorded',
    note_datetime = 'Date and time when the note was recorded',
    note_type_concept_id = 'Concept ID representing the type of note',
    note_class_concept_id = 'Concept ID representing the class of note',
    note_title = 'Title of the note',
    note_text = 'Full text of the note',
    encoding_concept_id = 'Concept ID for the encoding used in the note',
    language_concept_id = 'Concept ID for the language of the note',
    provider_id = 'Identifier for the provider who authored the note',
    visit_occurrence_id = 'Identifier for the visit occurrence associated with the note',
    visit_detail_id = 'Identifier for the visit detail associated with the note',
    note_source_value = 'Source value representing the note',
    note_event_id = 'Identifier for the event associated with the note',
    note_event_field_concept_id = 'Concept ID for the field of the event associated with the note'
  ) 
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
