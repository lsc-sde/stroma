
MODEL (
  name @temp_schema.achilles__s_tmpach_212,
  kind FULL,
  cron '@monthly',
  blueprints (
    (schema_achilles := silver_achilles, src_schema := silver, @temp_schema := z_tmp_silver_achilles),
    (schema_achilles := gold_achilles,  src_schema := gold, @temp_schema := z_tmp_gold_achilles)
)
, dialect 'databricks'
);

-- 212	Number of persons with at least one visit occurrence by calendar year by gender by age decile
with rawData as (
  select
    p1.gender_concept_id as stratum_2,
    YEAR(visit_start_date) as stratum_1,
    FLOOR((YEAR(visit_start_date) - p1.year_of_birth) / 10) as stratum_3,
    COUNT(distinct p1.PERSON_ID)::FLOAT as count_value
  from @src_schema.person as p1
  inner join
    @src_schema.visit_occurrence as vo1
    on p1.person_id = vo1.person_id
  group by
    YEAR(visit_start_date),
    p1.gender_concept_id,
    FLOOR((YEAR(visit_start_date) - p1.year_of_birth) / 10)
)

select
  212 as analysis_id,
  count_value,
  CAST(stratum_1 as VARCHAR(255)) as stratum_1,
  CAST(stratum_2 as VARCHAR(255)) as stratum_2,
  CAST(stratum_3 as VARCHAR(255)) as stratum_3,
  CAST(null as VARCHAR(255)) as stratum_4,
  CAST(null as VARCHAR(255)) as stratum_5
from rawData
