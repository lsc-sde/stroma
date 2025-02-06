MODEL (
  name gold.condition_occurrence,
  kind FULL,
  cron '@monthly',
  grain condition_occurrence_id,
  references (
    person_id,
    condition_concept_id AS concept_id,
    condition_type_concept_id AS concept_id,
    condition_status_concept_id AS concept_id
  ),
  description 'This model contains records of Events of a Person suggesting the presence of a disease or medical condition stated as a diagnosis, a sign, or a symptom, which is either observed by a Provider or reported by the patient.',
  column_descriptions (
    condition_occurrence_id = 'Unique identifier for each condition occurrence.',
    person_id = 'Unique identifier for each person.',
    condition_concept_id = 'Standard concept identifier for the condition.',
    condition_start_date = 'Date when the condition started.',
    condition_start_datetime = 'Datetime when the condition started.',
    condition_end_date = 'Date when the condition ended.',
    condition_end_datetime = 'Datetime when the condition ended.',
    condition_type_concept_id = ' This field can be used to determine the provenance of the Condition record, as in whether the condition was from an EHR system, insurance claim, registry, or other sources.',
    condition_status_concept_id = 'This concept represents the point during the visit the diagnosis was given (admitting diagnosis, final diagnosis), whether the diagnosis was determined due to laboratory findings, if the diagnosis was exclusionary, or if it was a preliminary diagnosis, among others.',
    stop_reason = 'The Stop Reason indicates why a Condition is no longer valid with respect to the purpose within the source data. Note that a Stop Reason does not necessarily imply that the condition is no longer occurring.',
    provider_id = 'The provider associated with condition record, e.g. the provider who made the diagnosis or the provider who recorded the symptom.',
    visit_occurrence_id = 'The visit during which the condition occurred.',
    visit_detail_id = 'The VISIT_DETAIL record during which the condition occurred. For example, if the person was in the ICU at the time of the diagnosis the VISIT_OCCURRENCE record would reflect the overall hospital stay and the VISIT_DETAIL record would reflect the ICU stay during the hospital visit.',
    condition_source_value = 'This field houses the verbatim value from the source data representing the condition that occurred. For example, this could be an ICD10 or Read code.',
    condition_source_concept_id = 'This is the concept representing the condition source value and may not necessarily be standard. This field is discouraged from use in analysis because it is not required to contain Standard Concepts that are used across the OHDSI community, and should only be used when Standard Concepts do not adequately represent the source detail for the Condition necessary for a given analytic use case. Consider using CONDITION_CONCEPT_ID instead to enable standardized analytics that can be consistent across the network.',
    condition_status_source_value = 'This field houses the verbatim value from the source data representing the condition status.'
  )
);

SELECT
  co.condition_occurrence_id::INT,
  co.person_id::INT,
  co.condition_concept_id::INT,
  co.condition_start_date::DATE,
  co.condition_start_datetime::TIMESTAMP,
  co.condition_end_date::DATE,
  co.condition_end_datetime::TIMESTAMP,
  co.condition_type_concept_id::INT,
  co.condition_status_concept_id::INT,
  co.stop_reason::TEXT,
  co.provider_id::INT,
  co.visit_occurrence_id::INT,
  co.visit_detail_id::INT,
  co.condition_source_value::TEXT,
  co.condition_source_concept_id::INT,
  co.condition_status_source_value::TEXT
FROM stg_gold.stg__condition_occurrence AS co
