MODEL (
  name silver.note_nlp,
  kind FULL,
  cron '@monthly',
  grain note_nlp_id,
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb')
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
