MODEL (
  name silver.specimen,
  kind FULL,
  cron '@monthly',
  grain specimen_id,
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb')
);

SELECT
  s.specimen_id::INT,
  s.person_id::INT,
  s.specimen_concept_id::INT,
  s.specimen_type_concept_id::INT,
  s.specimen_date::DATE,
  s.specimen_datetime::TIMESTAMP,
  s.quantity::REAL,
  s.unit_concept_id::INT,
  s.anatomic_site_concept_id::INT,
  s.disease_status_concept_id::INT,
  s.specimen_source_id::TEXT,
  s.specimen_source_value::TEXT,
  s.unit_source_value::TEXT,
  s.anatomic_site_source_value::TEXT,
  s.disease_status_source_value::TEXT
FROM bronze.specimen AS s
