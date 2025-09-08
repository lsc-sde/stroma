MODEL (
  name bronze_@{org}.visit_occurrence,
  kind FULL,
  cron '@monthly',
  grain visit_occurrence_id,
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb'),
  blueprints ((
      org := 'lth'
    ), (
      org := 'uhmb'
  ))
);

SELECT
  vo.visit_occurrence_id::BIGINT,
  vo.person_id::BIGINT,
  vo.visit_concept_id::BIGINT,
  vo.visit_start_date::DATE,
  vo.visit_start_datetime::TIMESTAMP,
  vo.visit_end_date::DATE,
  vo.visit_end_datetime::TIMESTAMP,
  vo.visit_type_concept_id::BIGINT,
  vo.provider_id::BIGINT,
  vo.care_site_id::BIGINT,
  vo.visit_source_value::TEXT,
  vo.visit_source_concept_id::BIGINT,
  vo.admitted_from_concept_id::BIGINT,
  vo.admitted_from_source_value::TEXT,
  vo.discharged_to_concept_id::BIGINT,
  vo.discharged_to_source_value::TEXT,
  vo.preceding_visit_occurrence_id::BIGINT,
  @org::TEXT AS org
FROM @catalog_src.@{org}_omop.visit_occurrence AS vo
