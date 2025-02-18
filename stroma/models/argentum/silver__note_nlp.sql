MODEL (
  name silver.note_nlp,
  kind FULL,
  cron '@monthly',
  grain note_nlp_id,
  references (
    section_concept_id AS concept_id,
    note_nlp_concept_id AS concept_id,
    note_nlp_source_concept_id AS concept_id
    ),
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb'),
  description 'The note_nlp table captures the results of natural language processing applied to clinical notes, detailing identified terms and their context.',
  column_descriptions (
    note_nlp_id = 'Unique identifier for each NLP processed note',
    note_id = 'Identifier linking to the original note',
    section_concept_id = 'Concept ID representing the section of the note',
    snippet = 'Extracted text snippet from the note',
    offset = 'Position of the snippet within the note',
    lexical_variant = 'Lexical variant of the term identified',
    note_nlp_concept_id = 'Concept ID for the identified term',
    note_nlp_source_concept_id = 'Source concept ID for the identified term',
    nlp_system = 'NLP system used for processing the note',
    nlp_date = 'Date when the note was processed by NLP',
    nlp_datetime = 'Date and time when the note was processed by NLP',
    term_exists = 'Indicator if the term exists in the note',
    term_temporal = 'Temporal context of the term in the note',
    term_modifiers = 'Modifiers providing additional context for the term'    
  )
);

SELECT
  nn.note_nlp_id::INT,
  nn.note_id::INT,
  nn.section_concept_id::INT,
  nn.snippet::TEXT,
  nn."offset"::TEXT,
  nn.lexical_variant::TEXT,
  nn.note_nlp_concept_id::INT,
  nn.note_nlp_source_concept_id::INT,
  nn.nlp_system::TEXT,
  nn.nlp_date::DATE,
  nn.nlp_datetime::TIMESTAMP,
  nn.term_exists::TEXT,
  nn.term_temporal::TEXT,
  nn.term_modifiers::TEXT
FROM bronze.note_nlp AS nn
