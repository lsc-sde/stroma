MODEL (
  name stg_gold.stg__specimen,
  kind VIEW,
  cron '@monthly'
);

SELECT
  sp.specimen_id,
  sp.person_id,
  sp.specimen_concept_id,
  sp.specimen_type_concept_id,
  sp.specimen_date,
  sp.specimen_datetime,
  sp.quantity,
  sp.unit_concept_id,
  sp.anatomic_site_concept_id,
  sp.disease_status_concept_id,
  sp.specimen_source_id,
  sp.specimen_source_value,
  sp.unit_source_value,
  sp.anatomic_site_source_value,
  sp.disease_status_source_value
FROM silver.specimen AS sp
INNER JOIN stg_gold.stg__person AS p
  ON sp.person_id = p.person_id
WHERE
  sp.specimen_date::DATE >= p.birth_datetime::DATE
  AND sp.specimen_date::DATE >= @minimum_observation_period_start_date
