MODEL (
  name stg_gold.stg__death,
  kind FULL,
  cron '@monthly'
);

SELECT
  d.person_id,
  d.death_date,
  d.death_datetime,
  d.death_type_concept_id,
  d.cause_concept_id,
  d.cause_source_value,
  d.cause_source_concept_id
FROM silver.death AS d
INNER JOIN stg_gold.stg__person AS p
  ON d.person_id = p.person_id
