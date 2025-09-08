MODEL (
  name stg_gold.stg__visit_detail,
  kind VIEW,
  cron '@monthly'
);

SELECT
  vd.visit_detail_id,
  vd.person_id,
  vd.visit_detail_concept_id,
  vd.visit_detail_start_date,
  vd.visit_detail_start_datetime,
  vd.visit_detail_end_date,
  vd.visit_detail_end_datetime,
  vd.visit_detail_type_concept_id,
  vd.provider_id,
  vd.care_site_id,
  vd.visit_detail_source_value,
  vd.visit_detail_source_concept_id,
  vd.admitted_from_source_value,
  vd.discharged_to_source_value,
  vd.discharged_to_concept_id,
  lag(vd.visit_detail_id) OVER (PARTITION BY vd.person_id ORDER BY vd.visit_detail_start_datetime) AS preceding_visit_detail_id,
  vd.parent_visit_detail_id,
  vd.visit_occurrence_id
FROM silver.visit_detail AS vd
INNER JOIN stg_gold.stg__person AS p
  ON vd.person_id = p.person_id
LEFT JOIN stg_gold.stg__death AS d
  ON vd.person_id = d.person_id
/* keep only visit detail for clean visit occurrences */
WHERE
  EXISTS(
    SELECT
      1
    FROM stg_gold.stg__visit_occurrence AS vo
    WHERE
      vo.visit_occurrence_id = vd.visit_occurrence_id
  )
  AND vd.visit_detail_start_date >= p.birth_datetime::DATE
  AND (
    vd.visit_detail_start_datetime >= p.birth_datetime
    OR vd.visit_detail_start_datetime IS NULL
  )
  AND (
    vd.visit_detail_start_date <= d.death_date OR d.death_date IS NULL
  )
  AND NOT vd.visit_detail_end_date IS NULL
  AND NOT vd.visit_detail_end_datetime IS NULL
  AND vd.visit_detail_end_date >= p.birth_datetime::DATE
  AND (
    vd.visit_detail_end_date <= d.death_date OR d.death_date IS NULL
  )
  AND vd.visit_detail_end_date::DATE <= CURRENT_DATE
  AND vd.visit_detail_end_datetime::TIMESTAMP <= CURRENT_TIMESTAMP
