MODEL (
  name gold.visit_detail,
  kind FULL,
  cron '@monthly',
  grain visit_detail_id,
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb')
);

SELECT
  vd.visit_detail_id::INT,
  vd.person_id::INT,
  vd.visit_detail_concept_id::INT,
  vd.visit_detail_start_date::DATE,
  vd.visit_detail_start_datetime::TIMESTAMP,
  vd.visit_detail_end_date::DATE,
  vd.visit_detail_end_datetime::TIMESTAMP,
  vd.visit_detail_type_concept_id::INT,
  vd.provider_id::INT,
  vd.care_site_id::INT,
  vd.visit_detail_source_value::TEXT,
  vd.visit_detail_source_concept_id::INT,
  vd.admitted_from_source_value::TEXT,
  vd.discharged_to_source_value::TEXT,
  vd.discharged_to_concept_id::INT,
  vd.preceding_visit_detail_id::INT,
  vd.parent_visit_detail_id::INT,
  vd.visit_occurrence_id::INT
FROM stg_gold.stg__visit_detail AS vd
