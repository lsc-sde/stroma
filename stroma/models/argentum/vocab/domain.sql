MODEL (
  name silver.domain,
  kind VIEW,
  cron '@yearly',
  grain domain_id
);

SELECT
  d.domain_id,
  d.domain_name,
  d.domain_concept_id
FROM bronze.domain AS d
