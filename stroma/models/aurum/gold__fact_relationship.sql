MODEL (
  name gold.fact_relationship,
  kind FULL,
  cron '@monthly',
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb')
);

SELECT
  fr.domain_concept_id_1,
  fr.fact_id_1,
  fr.domain_concept_id_2,
  fr.fact_id_2,
  fr.relationship_concept_id
FROM silver.fact_relationship AS fr
