MODEL (
  name stg_gold.stg__persons_with_facts,
  kind VIEW,
  cron '@monthly'
);

SELECT DISTINCT
  person_id::BIGINT
FROM (
  SELECT
    person_id
  FROM silver.measurement
  WHERE
    measurement_date >= @minimum_observation_period_start_date
  UNION
  SELECT
    person_id
  FROM silver.observation
  WHERE
    observation_date >= @minimum_observation_period_start_date
  UNION
  SELECT
    person_id
  FROM silver.condition_occurrence
  WHERE
    condition_start_date >= @minimum_observation_period_start_date
  UNION
  SELECT
    person_id
  FROM silver.procedure_occurrence
  WHERE
    procedure_date >= @minimum_observation_period_start_date
  UNION
  SELECT
    person_id
  FROM silver.drug_exposure
  WHERE
    drug_exposure_start_date >= @minimum_observation_period_start_date
  UNION
  SELECT
    person_id
  FROM silver.device_exposure
  WHERE
    device_exposure_start_date >= @minimum_observation_period_start_date
  UNION
  SELECT
    person_id
  FROM silver.visit_occurrence
  WHERE
    visit_start_date >= @minimum_observation_period_start_date
  UNION
  SELECT
    person_id
  FROM silver.specimen
  WHERE
    specimen_date >= @minimum_observation_period_start_date
)
