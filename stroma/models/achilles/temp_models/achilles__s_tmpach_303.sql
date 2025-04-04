
MODEL (
  name @temp_schema.achilles__s_tmpach_303,
  kind FULL,
  cron '@monthly',
  blueprints (
    (schema_achilles := silver_achilles, src_schema := silver, @temp_schema := z_tmp_silver_achilles),
    (schema_achilles := gold_achilles,  src_schema := gold, @temp_schema := z_tmp_gold_achilles)
)
, dialect 'databricks'
);

-- 303	Number of provider records, by specialty_concept_id, visit_concept_id
select
  303 as analysis_id,
  cast(p.specialty_concept_id as varchar(255)) as stratum_1,
  cast(vo.visit_concept_id as varchar(255)) as stratum_2,
  cast(null as varchar(255)) as stratum_3,
  cast(null as varchar(255)) as stratum_4,
  cast(null as varchar(255)) as stratum_5,
  count(*)::FLOAT as count_value
from @src_schema.provider as p
inner join @src_schema.visit_occurrence as vo
  on vo.provider_id = p.provider_id
group by p.specialty_concept_id, visit_concept_id
