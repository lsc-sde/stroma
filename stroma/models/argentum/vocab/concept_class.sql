MODEL (
  name silver.concept_class,
  kind VIEW,
  cron '@yearly'
);

SELECT
  cc.concept_class_id,
  cc.concept_class_name,
  cc.concept_class_concept_id
FROM bronze.concept_class AS cc
