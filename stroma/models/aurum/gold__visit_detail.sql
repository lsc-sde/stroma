MODEL (
  name gold.visit_detail,
  kind FULL,
  cron '@monthly',
  grain visit_detail_id,
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb'),
   description 'The visit_detail table captures detailed information about individual components of a healthcare visit, including specific encounters and services provided.',
  column_descriptions (
    visit_detail_id = 'Unique identifier for each visit detail',
    person_id = 'Identifier for the person associated with the visit detail',
    visit_detail_concept_id = 'Concept ID representing the visit detail',
    visit_detail_start_date = 'Start date of the visit detail',
    visit_detail_start_datetime = 'Start date and time of the visit detail',
    visit_detail_end_date = 'End date of the visit detail',
    visit_detail_end_datetime = 'End date and time of the visit detail',
    visit_detail_type_concept_id = 'Concept ID representing the type of visit detail',
    provider_id = 'Identifier for the provider associated with the visit detail',
    care_site_id = 'Identifier for the care site associated with the visit detail',
    visit_detail_source_value = 'Source value representing the visit detail',
    visit_detail_source_concept_id = 'Source concept ID for the visit detail',
    admitted_from_source_value = 'Source value for the admission origin',
    discharged_to_source_value = 'Source value for the discharge destination',
    discharged_to_concept_id = 'Concept ID for the discharge destination',
    preceding_visit_detail_id = 'Identifier for the preceding visit detail',
    parent_visit_detail_id = 'Identifier for the parent visit detail',
    visit_occurrence_id = 'Identifier for the visit occurrence associated with the visit detail'
  )
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
