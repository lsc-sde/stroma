
MODEL (
  name @temp_schema.achilles__s_tmpach_920,
  kind FULL,
  cron '@monthly',
  blueprints (
    (schema_achilles := silver_achilles, src_schema := silver, @temp_schema := z_tmp_silver_achilles),
    (schema_achilles := gold_achilles,  src_schema := gold, @temp_schema := z_tmp_gold_achilles)
)
, dialect 'databricks'
);

-- 920	Number of drug era records by drug era start month
with rawData as (
  select
    YEAR(de.drug_era_start_date) * 100
    + MONTH(de.drug_era_start_date) as stratum_1,
    COUNT(de.person_id)::FLOAT as count_value
  from
    @src_schema.drug_era as de
  inner join
    @src_schema.observation_period as op
    on
      de.person_id = op.person_id
      and
      de.drug_era_start_date >= op.observation_period_start_date
      and
      de.drug_era_start_date <= op.observation_period_end_date
  group by
    YEAR(de.drug_era_start_date) * 100 + MONTH(de.drug_era_start_date)
)

select
  920 as analysis_id,
  count_value,
  CAST(stratum_1 as VARCHAR(255)) as stratum_1,
  CAST(NULL as VARCHAR(255)) as stratum_2,
  CAST(NULL as VARCHAR(255)) as stratum_3,
  CAST(NULL as VARCHAR(255)) as stratum_4,
  CAST(NULL as VARCHAR(255)) as stratum_5
from
  rawData
