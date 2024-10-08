MODEL (
  name @schema_stg.stg__persons_with_facts,
  kind FULL,
  cron '@monthly'
);

SELECT DISTINCT
  person_id::BIGINT
FROM (
  SELECT
    person_id
  FROM @schema_src.measurement
  WHERE
    measurement_date >= @minimum_observation_period_start_date
  UNION
  SELECT
    person_id
  FROM @schema_src.observation
  WHERE
    observation_date >= @minimum_observation_period_start_date
  UNION
  SELECT
    person_id
  FROM @schema_src.condition_occurrence
  WHERE
    condition_start_date >= @minimum_observation_period_start_date
  UNION
  SELECT
    person_id
  FROM @schema_src.procedure_occurrence
  WHERE
    procedure_date >= @minimum_observation_period_start_date
  UNION
  SELECT
    person_id
  FROM @schema_src.drug_exposure
  WHERE
    drug_exposure_start_date >= @minimum_observation_period_start_date
  UNION
  SELECT
    person_id
  FROM @schema_src.device_exposure
  WHERE
    device_exposure_start_date >= @minimum_observation_period_start_date
  UNION
  SELECT
    person_id
  FROM @schema_src.visit_occurrence
  WHERE
    visit_start_date >= @minimum_observation_period_start_date
  UNION
  SELECT
    person_id
  FROM @schema_src.specimen
  WHERE
    specimen_date >= @minimum_observation_period_start_date
)
