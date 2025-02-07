MODEL (
  name gold.location,
  kind FULL,
  cron '@monthly',
  grain location_id,
  physical_properties ('delta.tuneFileSizesForRewrites' = FALSE, 'delta.targetFileSize' = '256mb')
);

SELECT
  l.location_id::INT,
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
FROM silver.location AS l
