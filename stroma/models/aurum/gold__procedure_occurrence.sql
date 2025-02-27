MODEL (
  name gold.procedure_occurrence,
  kind FULL,
  cron '@monthly',
  grain procedure_occurrence_id,
  references (
    person_id,
    procedure_concept_id AS concept_id, 
    procedure_type_concept_id AS concept_id,
    modifier_concept_id AS concept_id,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    procedure_source_concept_id AS concept_id
    ),
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb'),
  description 'The procedure_occurrence table captures details about procedures performed on a person, including the type, date, and provider of the procedure.', 
  column_descriptions (
      procedure_occurrence_id = 'Unique identifier for each procedure occurrence',
      person_id = 'Identifier for the person who underwent the procedure',
      procedure_concept_id = 'Concept ID representing the procedure',
      procedure_date = 'Date when the procedure was performed',
      procedure_datetime = 'Date and time when the procedure was performed',
      procedure_end_date = 'End date of the procedure',
      procedure_end_datetime = 'End date and time of the procedure',
      procedure_type_concept_id = 'Concept ID representing the type of procedure',
      modifier_concept_id = 'Concept ID for any modifiers applied to the procedure',
      quantity = 'Quantity of the procedure performed',
      provider_id = 'Identifier for the provider who performed the procedure',
      visit_occurrence_id = 'Identifier for the visit occurrence associated with the procedure',
      visit_detail_id = 'Identifier for the visit detail associated with the procedure',
      procedure_source_value = 'Source value representing the procedure',
      procedure_source_concept_id = 'Source concept ID for the procedure',
      modifier_source_value = 'Source value for any modifiers applied to the procedure'
  )
);

SELECT
  po.procedure_occurrence_id::INT,
  po.person_id::INT,
  po.procedure_concept_id::INT,
  po.procedure_date::DATE,
  po.procedure_datetime::TIMESTAMP,
  po.procedure_end_date::DATE,
  po.procedure_end_datetime::TIMESTAMP,
  po.procedure_type_concept_id::INT,
  po.modifier_concept_id::INT,
  po.quantity::INT,
  po.provider_id::INT,
  po.visit_occurrence_id::INT,
  po.visit_detail_id::INT,
  po.procedure_source_value::TEXT,
  po.procedure_source_concept_id::INT,
  po.modifier_source_value::TEXT
FROM silver.procedure_occurrence AS po
