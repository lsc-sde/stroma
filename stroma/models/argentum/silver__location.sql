MODEL (
  name silver.location,
  kind FULL,
  cron '@monthly',
  grain location_id,
  references (
    country_concept_id AS concept_id
    ),
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb'),
  description 'Table containing geographic and address information for persons and care sites in the OMOP CDM',
  column_descriptions (
      location_id = 'Unique identifier for each location',
      address_1 = 'Primary address line',
      address_2 = 'Secondary address line',
      city = 'City of the location',
      [state] = 'State or province of the location',
      zip = 'Postal code of the location',
      county = 'County of the location',
      location_source_value = 'Source value for the location',
      country_concept_id = 'Concept ID representing the country',
      country_source_value = 'Source value for the country',
      latitude = 'Latitude coordinate of the location',
      longitude = 'Longitude coordinate of the location'
  )
);

SELECT
  l.location_id::BIGINT,
  l.address_1::TEXT,
  l.address_2::TEXT,
  l.city::TEXT,
  l.state::TEXT,
  l.zip::TEXT,
  l.county::TEXT,
  l.location_source_value::TEXT,
  l.country_concept_id::INT,
  l.country_source_value::TEXT,
  l.latitude::REAL,
  l.longitude::REAL
FROM bronze.location AS l
