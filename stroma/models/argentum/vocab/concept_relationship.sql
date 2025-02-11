MODEL (
  name silver.concept_relationship,
  kind VIEW,
  cron '@yearly'
);

SELECT
  cr.concept_id_1::INT,
  cr.concept_id_2::INT,
  cr.relationship_id::TEXT,
  cr.valid_start_date::DATE,
  cr.valid_end_date::DATE,
  cr.invalid_reason::TEXT
FROM bronze.concept_relationship AS cr
