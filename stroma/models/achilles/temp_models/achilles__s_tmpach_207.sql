
MODEL (
  name @temp_schema.achilles__s_tmpach_207,
  kind FULL,
  cron '@monthly',
  blueprints (
    (schema_achilles := silver_achilles, src_schema := silver, @temp_schema := z_tmp_silver_achilles),
    (schema_achilles := gold_achilles,  src_schema := gold, @temp_schema := z_tmp_gold_achilles)
)
);

--207	Number of visit records with invalid person_id
select
  207 as analysis_id,
  cast(null as varchar(255)) as stratum_1,
  cast(null as varchar(255)) as stratum_2,
  cast(null as varchar(255)) as stratum_3,
  cast(null as varchar(255)) as stratum_4,
  cast(null as varchar(255)) as stratum_5,
  count(vo1.PERSON_ID)::FLOAT as count_value
from
  @src_schema.visit_occurrence as vo1
left join @src_schema.person as p1
  on p1.person_id = vo1.person_id
where p1.person_id is null
