
MODEL (
  name @temp_schema.achilles__s_tmpach_702,
  kind FULL,
  cron '@monthly',
  blueprints (
    (schema_achilles := silver_achilles, src_schema := silver, @temp_schema := z_tmp_silver_achilles),
    (schema_achilles := gold_achilles,  src_schema := gold, @temp_schema := z_tmp_gold_achilles)
)
);

-- 702	Number of persons by drug occurrence start month, by drug_concept_id
with rawData as (
  select
    de.drug_concept_id as stratum_1,
    YEAR(de.drug_exposure_start_date) * 100
    + MONTH(de.drug_exposure_start_date) as stratum_2,
    COUNT(distinct de.person_id)::FLOAT as count_value
  from
    @src_schema.drug_exposure as de
  inner join
    @src_schema.observation_period as op
    on
      de.person_id = op.person_id
      and
      de.drug_exposure_start_date >= op.observation_period_start_date
      and
      de.drug_exposure_start_date <= op.observation_period_end_date
  group by
    de.drug_concept_id,
    YEAR(de.drug_exposure_start_date) * 100 + MONTH(de.drug_exposure_start_date)
)

select
  702 as analysis_id,
  count_value,
  CAST(stratum_1 as VARCHAR(255)) as stratum_1,
  CAST(stratum_2 as VARCHAR(255)) as stratum_2,
  CAST(NULL as VARCHAR(255)) as stratum_3,
  CAST(NULL as VARCHAR(255)) as stratum_4,
  CAST(NULL as VARCHAR(255)) as stratum_5
from
  rawData
