MODEL (
  name gold.domain,
  kind VIEW,
  cron '@yearly',
  grain domain_id
);

SELECT
  d.domain_id,
  d.domain_name,
  d.domain_concept_id
FROM silver.domain AS d
