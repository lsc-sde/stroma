MODEL (
  name silver.domain,
  kind VIEW,
  cron '@yearly'
);

SELECT
  d.domain_id::TEXT,
  d.domain_name::TEXT,
  d.domain_concept_id::INT
FROM bronze.domain AS d
