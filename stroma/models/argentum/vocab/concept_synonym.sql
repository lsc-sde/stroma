MODEL (
  name silver.concept_synonym,
  kind VIEW,
  cron '@yearly'
);

SELECT
  cs.concept_id::INT,
  cs.concept_synonym_name::TEXT,
  cs.language_concept_id::INT
FROM bronze.concept_synonym AS cs
