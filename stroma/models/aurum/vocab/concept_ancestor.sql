MODEL (
  name gold.concept_ancestor,
  kind VIEW,
  cron '@yearly'
);

SELECT
  ca.ancestor_concept_id::INT,
  ca.descendant_concept_id::INT,
  ca.min_levels_of_separation::INT,
  ca.max_levels_of_separation::INT
FROM silver.concept_ancestor AS ca
