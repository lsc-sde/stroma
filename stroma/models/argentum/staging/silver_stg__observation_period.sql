MODEL (
  name stg_silver.stg__observation_period,
  kind VIEW,
  cron '@monthly'
);

@DEF(layer, 'silver');

WITH spans AS (
  SELECT
    *
  FROM (
    @get_observation_period(@layer.condition_era, condition_era_start_date, condition_era_end_date)
  )
  UNION
  SELECT
    *
  FROM (
    @get_observation_period(@layer.device_exposure, device_exposure_start_date, device_exposure_start_date)
  )
  UNION
  SELECT
    *
  FROM (
    @get_observation_period(@layer.drug_exposure, drug_exposure_start_date, drug_exposure_start_date)
  )
  UNION
  SELECT
    *
  FROM (
    @get_observation_period(@layer.measurement, measurement_date, measurement_date)
  )
  UNION
  SELECT
    *
  FROM (
    @get_observation_period(@layer.observation, observation_date, observation_date)
  )
  UNION
  SELECT
    *
  FROM (
    @get_observation_period(@layer.procedure_occurrence, procedure_date, procedure_date)
  )
  UNION
  SELECT
    *
  FROM (
    @get_observation_period(@layer.specimen, specimen_date, specimen_date)
  )
  UNION
  SELECT
    *
  FROM (
    @get_observation_period(@layer.visit_occurrence, visit_start_date, visit_end_date)
  )
), observation_period AS (
  SELECT
    person_id,
    observation_period_start_date,
    observation_period_end_date,
    row_number() OVER (PARTITION BY person_id ORDER BY observation_period_start_date) AS row_num,
    date_diff(
      'DAY',
      lag(observation_period_end_date) OVER (PARTITION BY person_id ORDER BY observation_period_start_date)::DATE,
      observation_period_start_date::DATE
    ) AS time_difference
  FROM spans
  WHERE
    observation_period_start_date >= @minimum_observation_period_start_date
), grouped_data AS (
  SELECT
    *,
    sum(CASE WHEN time_difference > 548 OR time_difference IS NULL THEN 1 ELSE 0 END) OVER (PARTITION BY person_id ORDER BY row_num) AS group_id
  FROM observation_period
)
SELECT DISTINCT
  person_id,
  first_value(observation_period_start_date) OVER (PARTITION BY person_id, group_id ORDER BY observation_period_start_date) AS observation_period_start_date,
  first_value(observation_period_end_date) OVER (PARTITION BY person_id, group_id ORDER BY observation_period_start_date DESC) AS observation_period_end_date
FROM grouped_data
