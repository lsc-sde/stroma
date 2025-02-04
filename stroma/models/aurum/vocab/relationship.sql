MODEL (
  name gold.relationship,
  kind VIEW,
  cron '@yearly',
  grain relationship_id
);

SELECT
  r.relationship_id,
  r.relationship_name,
  r.is_hierarchical,
  r.defines_ancestry,
  r.reverse_relationship_id,
  r.relationship_concept_id
FROM silver.relationship AS r
