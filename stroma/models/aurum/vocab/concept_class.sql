MODEL (
  name gold.concept_class,
  kind VIEW,
  cron '@yearly',
  grain concept_class_id
);

SELECT
  cc.concept_class_id::TEXT,
  cc.concept_class_name::TEXT,
  cc.concept_class_concept_id::INT
FROM silver.concept_class AS cc
