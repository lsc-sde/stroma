MODEL (
  name silver.vocabulary,
  kind VIEW,
  cron '@yearly',
  grain vocabulary_id
);

SELECT
  v.vocabulary_id,
  v.vocabulary_name,
  v.vocabulary_reference,
  v.vocabulary_version,
  v.vocabulary_concept_id
FROM bronze.vocabulary AS v
