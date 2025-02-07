MODEL (
  name silver.visit_occurrence,
  kind FULL,
  cron '@monthly',
  grain visit_occurrence_id,
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb')
);

SELECT
  vo.visit_occurrence_id::INT,
  vo.person_id::INT,
  vo.visit_concept_id::INT,
  vo.visit_start_date::DATE,
  vo.visit_start_datetime::TIMESTAMP,
  vo.visit_end_date::DATE,
  vo.visit_end_datetime::TIMESTAMP,
  vo.visit_type_concept_id::INT,
  vo.provider_id::INT,
  vo.care_site_id::INT,
  vo.visit_source_value::TEXT,
  vo.visit_source_concept_id::INT,
  vo.admitted_from_concept_id::INT,
  vo.admitted_from_source_value::TEXT,
  vo.discharged_to_concept_id::INT,
  vo.discharged_to_source_value::TEXT,
  vo.preceding_visit_occurrence_id::INT
FROM bronze.visit_occurrence AS vo
