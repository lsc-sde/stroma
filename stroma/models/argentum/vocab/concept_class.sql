MODEL (
  name silver.concept_class,
  kind VIEW,
  cron '@yearly'
);

SELECT
  cc.concept_class_id::TEXT,
  cc.concept_class_name::TEXT,
  cc.concept_class_concept_id::INT
FROM bronze.concept_class AS cc
