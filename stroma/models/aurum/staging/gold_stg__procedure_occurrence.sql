MODEL (
  name stg_gold.stg__procedure_occurrence,
  kind VIEW,
  cron '@monthly'
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
INNER JOIN stg_gold.stg__person AS p
  ON po.person_id = p.person_id
INNER JOIN stg_gold.stg__visit_occurrence AS vo
  ON po.visit_occurrence_id = vo.visit_occurrence_id
WHERE
  po.procedure_date >= p.birth_datetime::DATE
  AND NOT po.procedure_date IS NULL
  AND po.procedure_date >= @minimum_observation_period_start_date
