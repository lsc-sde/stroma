MODEL (
  name silver.visit_occurrence,
  kind FULL,
  cron '@monthly',
  grain visit_occurrence_id,
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb'),
  description 'The visit_occurrence table captures details about each healthcare visit, including the type, date, and duration of the visit.',
  column_descriptions (
    visit_occurrence_id = 'Unique identifier for each visit occurrence',
    person_id = 'Identifier for the person associated with the visit',
    visit_concept_id = 'Concept ID representing the type of visit',
    visit_start_date = 'Start date of the visit',
    visit_start_datetime = 'Start date and time of the visit',
    visit_end_date = 'End date of the visit',
    visit_end_datetime = 'End date and time of the visit',
    visit_type_concept_id = 'Concept ID representing the type of visit',
    provider_id = 'Identifier for the provider associated with the visit',
    care_site_id = 'Identifier for the care site associated with the visit',
    visit_source_value = 'Source value representing the visit',
    visit_source_concept_id = 'Source concept ID for the visit',
    admitted_from_concept_id = 'Concept ID for the admission origin',
    admitted_from_source_value = 'Source value for the admission origin',
    discharged_to_concept_id = 'Concept ID for the discharge destination',
    discharged_to_source_value = 'Source value for the discharge destination',
    preceding_visit_occurrence_id = 'Identifier for the preceding visit occurrence'
  )
);

SELECT
  vo.visit_occurrence_id,
  vo.person_id,
  vo.visit_concept_id,
  vo.visit_start_date,
  vo.visit_start_datetime,
  vo.visit_end_date,
  vo.visit_end_datetime,
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
FROM bronze.visit_occurrence AS vo
