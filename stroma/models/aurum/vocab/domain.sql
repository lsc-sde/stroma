MODEL (
  name gold.domain,
  kind VIEW,
  cron '@yearly',
  grain domain_id
);

SELECT
  d.domain_id::TEXT,
  d.domain_name::TEXT,
  d.domain_concept_id::INT
FROM silver.domain AS d
