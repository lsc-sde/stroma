MODEL (
  name gold.relationship,
  kind VIEW,
  cron '@yearly'
);

SELECT
  r.relationship_id::TEXT,
  r.relationship_name::TEXT,
  r.is_hierarchical::TEXT,
  r.defines_ancestry::TEXT,
  r.reverse_relationship_id::TEXT,
  r.relationship_concept_id::INT
FROM silver.relationship AS r
