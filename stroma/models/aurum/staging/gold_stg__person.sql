MODEL (
  name stg_gold.stg__person,
  kind VIEW,
  cron '@monthly'
);

/* ToDo: Add logic for opt-outs */
SELECT
  p.person_id,
  p.gender_concept_id,
  p.year_of_birth,
  p.month_of_birth,
  p.day_of_birth,
  p.birth_datetime::DATE AS birth_datetime,
  p.race_concept_id,
  p.ethnicity_concept_id,
  p.location_id,
  p.provider_id,
  p.care_site_id,
  p.person_source_value,
  p.gender_source_value,
  p.gender_source_concept_id,
  p.race_source_value,
  p.race_source_concept_id,
  p.ethnicity_source_value,
  p.ethnicity_source_concept_id
FROM silver.person AS p
WHERE
  EXISTS(
    SELECT
      1
    FROM stg_gold.stg__persons_with_facts AS op
    WHERE
      op.person_id = p.person_id
  )
  AND NOT p.person_id IN (
    SELECT
      o.person_id
    FROM silver.observation AS o
    WHERE
      o.observation_concept_id = 44787910
  )
  AND p.birth_datetime <= CURRENT_DATE /* patients cannot be born in the future */
  AND p.gender_concept_id IN (8532, 8507) /* gender concept must be female or male */
