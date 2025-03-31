MODEL (
  name @schema_achilles.achilles_analysis_details,
  kind SEED (
    path '$root/seeds/achilles/achilles_analysis_details.csv'
  ),
  blueprints (
    (schema_achilles := silver_achilles),
    (schema_achilles := silver_gold)
    )
)
