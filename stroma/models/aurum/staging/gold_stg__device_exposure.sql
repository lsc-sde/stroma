MODEL (
  name stg_gold.stg__device_exposure,
  kind VIEW,
  cron '@monthly'
);

SELECT
  de.device_exposure_id,
  de.person_id,
  de.device_concept_id,
  de.device_exposure_start_date,
  de.device_exposure_start_datetime,
  de.device_exposure_end_date,
  de.device_exposure_end_datetime,
  de.device_type_concept_id,
  de.unique_device_id,
  de.production_id,
  de.quantity,
  de.provider_id,
  de.visit_occurrence_id,
  de.visit_detail_id,
  de.device_source_value,
  de.device_source_concept_id,
  de.unit_concept_id,
  de.unit_source_value,
  de.unit_source_concept_id
FROM silver.device_exposure AS de
INNER JOIN stg_gold.stg__person AS p
  ON de.person_id = p.person_id
INNER JOIN stg_gold.stg__visit_occurrence AS vo
  ON de.visit_occurrence_id = vo.visit_occurrence_id
WHERE
  de.device_exposure_start_date >= p.birth_datetime::DATE
  AND NOT de.device_exposure_start_date IS NULL
  AND de.device_exposure_start_date >= @minimum_observation_period_start_date
