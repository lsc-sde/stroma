MODEL (
  name @schema_achilles.achilles_analysis_details,
  kind SEED (
    path '$root/seeds/achilles/achilles_analysis_details.csv'
  ),
  blueprints (
    (schema_achilles := silver_achilles, src_schema := silver, @temp_schema := z_tmp_silver_achilles),
    (schema_achilles := gold_achilles,  src_schema := gold, @temp_schema := z_tmp_gold_achilles)
)
, dialect 'databricks'
);
