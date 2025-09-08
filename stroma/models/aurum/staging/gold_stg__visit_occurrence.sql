MODEL (
  name stg_gold.stg__visit_occurrence,
  kind VIEW,
  cron '@monthly'
);

WITH gold_visits AS (
  SELECT
    vo.visit_occurrence_id,
    vo.person_id,
    vo.visit_concept_id,
    vo.visit_start_date,
    vo.visit_start_datetime,
    CASE
      WHEN NOT vo.visit_end_date IS NULL
      THEN vo.visit_end_date
      WHEN vo.visit_source_value = 'PUI' AND NOT d.death_datetime IS NULL
      THEN d.death_datetime
      WHEN vo.visit_source_value = 'PUI'
      THEN CURRENT_DATE
    END AS visit_end_date,
    CASE
      WHEN NOT vo.visit_end_date IS NULL
      THEN vo.visit_end_datetime
      WHEN vo.visit_source_value = 'PUI' AND NOT d.death_datetime IS NULL
      THEN d.death_datetime
      WHEN vo.visit_source_value = 'PUI'
      THEN CURRENT_TIMESTAMP
    END AS visit_end_datetime,
    vo.visit_type_concept_id,
    vo.provider_id,
    vo.care_site_id,
    vo.visit_source_value,
    vo.visit_source_concept_id,
    vo.admitted_from_concept_id,
    vo.admitted_from_source_value,
    vo.discharged_to_concept_id,
    vo.discharged_to_source_value,
    vo.preceding_visit_occurrence_id
  FROM silver.visit_occurrence AS vo
  LEFT JOIN silver.death AS d
    ON vo.person_id = d.person_id
)
SELECT
  vo.visit_occurrence_id::BIGINT,
  vo.person_id::BIGINT,
  vo.visit_concept_id,
  vo.visit_start_date::DATE,
  vo.visit_start_datetime::TIMESTAMP,
  vo.visit_end_date::DATE,
  vo.visit_end_datetime::TIMESTAMP,
  vo.visit_type_concept_id,
  vo.provider_id,
  vo.care_site_id,
  vo.visit_source_value,
  vo.visit_source_concept_id,
  vo.admitted_from_concept_id,
  vo.admitted_from_source_value,
  vo.discharged_to_concept_id,
  vo.discharged_to_source_value,
  lag(vo.visit_occurrence_id) OVER (PARTITION BY vo.person_id ORDER BY vo.visit_start_datetime) AS preceding_visit_occurrence_id
FROM gold_visits AS vo
INNER JOIN stg_gold.stg__person AS p
  ON vo.person_id = p.person_id
LEFT JOIN stg_gold.stg__death AS d
  ON vo.person_id = d.person_id
WHERE
  vo.visit_start_date >= p.birth_datetime::DATE
  AND (
    vo.visit_start_datetime >= p.birth_datetime OR vo.visit_start_datetime IS NULL
  )
  AND (
    vo.visit_start_date <= d.death_date OR d.death_date IS NULL
  )
  AND NOT vo.visit_end_date IS NULL
  AND NOT vo.visit_end_datetime IS NULL
  AND vo.visit_end_date >= p.birth_datetime::DATE
  AND (
    vo.visit_end_date <= d.death_date OR d.death_date IS NULL
  )
  AND vo.visit_end_date <= CURRENT_DATE
  AND vo.visit_end_datetime <= CURRENT_TIMESTAMP
  AND NOT vo.visit_concept_id IS NULL
  AND vo.visit_start_date >= @minimum_observation_period_start_date
