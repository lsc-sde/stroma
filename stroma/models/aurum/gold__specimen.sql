MODEL (
  name gold.specimen,
  kind FULL,
  cron '@monthly',
  grain specimen_id,
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb'),
  description 'The specimen table contains details about biological specimens collected from patients, including type, collection date, and source.',
  column_descriptions (
    specimen_id = 'Unique identifier for each specimen',
    person_id = 'Identifier for the person from whom the specimen was collected',
    specimen_concept_id = 'Concept ID representing the type of specimen',
    specimen_type_concept_id = 'Concept ID representing the type of specimen collection',
    specimen_date = 'Date when the specimen was collected',
    specimen_datetime = 'Date and time when the specimen was collected',
    quantity = 'Quantity of the specimen collected',
    unit_concept_id = 'Concept ID for the unit of measurement',
    anatomic_site_concept_id = 'Concept ID for the anatomical site of specimen collection',
    disease_status_concept_id = 'Concept ID for the disease status of the specimen',
    specimen_source_id = 'Source identifier for the specimen',
    specimen_source_value = 'Source value representing the specimen',
    unit_source_value = 'Source value for the unit of measurement',
    anatomic_site_source_value = 'Source value for the anatomical site of specimen collection',
    disease_status_source_value = 'Source value for the disease status of the specimen'  
  )
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
FROM stg_gold.stg__specimen AS s
