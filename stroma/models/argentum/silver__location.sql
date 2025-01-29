MODEL (
  name silver.location,
  kind FULL,
  cron '@monthly',
  grain location_id,
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
  l.location_id,
  l.address_1,
  l.address_2,
  l.city,
  l.state,
  l.zip,
  l.county,
  l.location_source_value,
  l.country_concept_id,
  l.country_source_value,
  l.latitude,
  l.longitude
FROM bronze.location AS l
