MODEL (
  name silver.fact_relationship,
  kind FULL,
  cron '@monthly',
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb')
);

SELECT
  fr.domain_concept_id_1::INT,
  fr.fact_id_1::INT,
  fr.domain_concept_id_2::INT,
  fr.fact_id_2::INT,
  fr.relationship_concept_id::INT
FROM bronze.fact_relationship AS fr
