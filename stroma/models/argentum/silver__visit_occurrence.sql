MODEL (
  name silver.visit_occurrence,
  kind VIEW,
  cron '@monthly',
  grain visit_occurrence_id,
  references (
    person_id,
    visit_concept_id AS concept_id,
    visit_type_concept_id AS concept_id,
    provider_id,
    care_site_id,
    visit_source_concept_id AS concept_id,
    admitted_from_concept_id AS concept_id,
    discharged_to_concept_id AS concept_id,
    preceding_visit_occurrence_id AS visit_occurrence_id
  ),
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
  ),
  audits (
    not_null(columns := (person_id, visit_occurrence_id, visit_concept_id)),
    not_null_non_blocking(columns := (
      visit_start_date
    )),
    unique_values(columns := (
      visit_occurrence_id
    )),
    event_not_in_future_non_blocking("column" := visit_start_date),
    event_not_in_future_non_blocking("column" := visit_end_date)
  )
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
  vo.preceding_visit_occurrence_id::BIGINT
FROM bronze.visit_occurrence AS vo
JOIN silver.person AS p
  ON vo.person_id = p.person_id
LEFT JOIN silver.death AS d
  ON vo.person_id = d.person_id
WHERE
  vo.visit_start_date >= p.birth_datetime::DATE
  AND vo.visit_end_date <= coalesce(d.death_date, CURRENT_DATE)
