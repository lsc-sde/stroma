MODEL (
  name stg_gold.stg__drug_exposure,
  kind VIEW,
  cron '@monthly'
);

SELECT
  de.drug_exposure_id,
  de.person_id,
  de.drug_concept_id,
  de.drug_exposure_start_date,
  de.drug_exposure_start_datetime,
  de.drug_exposure_end_date,
  de.drug_exposure_end_datetime,
  de.verbatim_end_date,
  de.drug_type_concept_id,
  de.stop_reason,
  de.refills,
  de.quantity,
  de.days_supply,
  de.sig,
  de.route_concept_id,
  de.lot_number,
  de.provider_id,
  de.visit_occurrence_id,
  de.visit_detail_id,
  de.drug_source_value,
  de.drug_source_concept_id,
  de.route_source_value,
  de.dose_unit_source_value
FROM silver.drug_exposure AS de
INNER JOIN stg_gold.stg__person AS p
  ON de.person_id = p.person_id
INNER JOIN stg_gold.stg__visit_occurrence AS vo
  ON de.visit_occurrence_id = vo.visit_occurrence_id
WHERE
  de.drug_exposure_start_date >= p.birth_datetime::DATE
  AND NOT de.drug_exposure_start_date IS NULL
  AND de.drug_exposure_start_date >= @minimum_observation_period_start_date
