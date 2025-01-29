MODEL (
  name silver.fact_relationship,
  kind FULL,
  cron '@monthly',
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb'),
  description 'Table capturing relationships between facts in the OMOP CDM',
  column_descriptions (
    domain_concept_id_1 = 'Concept ID representing the domain of the first fact',
    fact_id_1 = 'Unique identifier for the first fact',
    domain_concept_id_2 = 'Concept ID representing the domain of the second fact',
    fact_id_2 = 'Unique identifier for the second fact',
    relationship_concept_id = 'Concept ID representing the relationship between the two facts'
  )
);

SELECT
  fr.domain_concept_id_1,
  fr.fact_id_1,
  fr.domain_concept_id_2,
  fr.fact_id_2,
  fr.relationship_concept_id
FROM bronze.fact_relationship AS fr
