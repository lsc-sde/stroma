MODEL (
  name @schema_temp.temp__condition_dates,
  kind FULL,
  cron '@monthly',
  grain person_id
);

SELECT
  person_id,
  observation_period_start_date,
  observation_period_end_date
FROM (
  @get_observation_period(@schema_dest.condition_era, condition_era_start_date, condition_era_end_date)
)
