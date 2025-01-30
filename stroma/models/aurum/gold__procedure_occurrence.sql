MODEL (
  name gold.procedure_occurrence,
  kind FULL,
  cron '@monthly',
  grain procedure_occurrence_id,
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
  po.procedure_occurrence_id,
  po.person_id,
  po.procedure_concept_id,
  po.procedure_date,
  po.procedure_datetime,
  po.procedure_end_date,
  po.procedure_end_datetime,
  po.procedure_type_concept_id,
  po.modifier_concept_id,
  po.quantity,
  po.provider_id,
  po.visit_occurrence_id,
  po.visit_detail_id,
  po.procedure_source_value,
  po.procedure_source_concept_id,
  po.modifier_source_value
FROM silver.procedure_occurrence AS po
