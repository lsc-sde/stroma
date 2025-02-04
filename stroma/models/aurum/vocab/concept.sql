MODEL (
  name gold.concept,
  kind VIEW,
  cron '@yearly',
  grain concept_id
);

SELECT
  c.concept_id,
  c.concept_name,
  c.domain_id,
  c.vocabulary_id,
  c.concept_class_id,
  c.standard_concept,
  c.concept_code,
  c.valid_start_date,
  c.valid_end_date,
  c.invalid_reason
FROM silver.concept AS c
