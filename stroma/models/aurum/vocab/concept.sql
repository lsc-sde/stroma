MODEL (
  name gold.concept,
  kind VIEW,
  cron '@yearly'
);

SELECT
  c.concept_id::INT,
  c.concept_name::TEXT,
  c.domain_id::TEXT,
  c.vocabulary_id::TEXT,
  c.concept_class_id::TEXT,
  c.standard_concept::TEXT,
  c.concept_code::TEXT,
  c.valid_start_date::DATE,
  c.valid_end_date::DATE,
  c.invalid_reason::TEXT
FROM silver.concept AS c
