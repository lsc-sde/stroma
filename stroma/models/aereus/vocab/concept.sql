MODEL (
  name bronze.concept,
  kind VIEW,
  cron '@yearly'
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
FROM @catalog_src.@base.concept AS c
