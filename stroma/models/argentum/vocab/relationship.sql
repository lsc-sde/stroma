MODEL (
  name silver.relationship,
  kind VIEW,
  cron '@yearly',
  grain relationship_id
);

SELECT
  r.relationship_id::TEXT,
  r.relationship_name::TEXT,
  r.is_hierarchical::TEXT,
  r.defines_ancestry::TEXT,
  r.reverse_relationship_id::TEXT,
  r.relationship_concept_id::INT
FROM bronze.relationship AS r
