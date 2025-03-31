
MODEL (
  name @temp_schema.achilles__s_tmpach_814,
  kind FULL,
  cron '@monthly',
  blueprints (
    (schema_achilles := silver_achilles, src_schema := silver, @temp_schema := z_tmp_silver_achilles),
    (schema_achilles := gold_achilles,  src_schema := gold, @temp_schema := z_tmp_gold_achilles)
)
);

-- 814	Number of observation records with no value (numeric, string, or concept)
select
  814 as analysis_id,
  cast(null as varchar(255)) as stratum_1,
  cast(null as varchar(255)) as stratum_2,
  cast(null as varchar(255)) as stratum_3,
  cast(null as varchar(255)) as stratum_4,
  cast(null as varchar(255)) as stratum_5,
  count(o1.PERSON_ID)::FLOAT as count_value
from
  @src_schema.observation as o1
where
  o1.value_as_number is null
  and o1.value_as_string is null
  and o1.value_as_concept_id is null
