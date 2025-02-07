MODEL (
  name silver.procedure_occurrence,
  kind FULL,
  cron '@monthly',
  grain procedure_occurrence_id,
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb')
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
FROM bronze.procedure_occurrence AS po
