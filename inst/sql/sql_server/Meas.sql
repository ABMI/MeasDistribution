IF OBJECT_ID('tempdb..#measure_summary', 'U') IS NOT NULL
drop table #measure_summary;

create table #measure_summary

select measurement_concept_id, count(distinct source_name) as num_sources, sum(num_persons) as num_persons, sum(num_records) as num_records
from
(
  select '@source_name' as source_name, measurement_concept_id, count(distinct person_id) as num_persons, count(person_id) as num_records
  from @cdm_schema.measurement
  where measurement_concept_id > 0
  group by measurement_concept_id
  
) t1
group by measurement_concept_id
;


IF OBJECT_ID('tempdb..#measure_van_summary', 'U') IS NOT NULL
drop table #measure_van_summary;
create table #measure_van_summary with (location=user_db, distribution=replicate) as
select measurement_concept_id, count(distinct source_name) as num_sources, sum(num_persons) as num_persons, sum(num_records) as num_records
from
(
  select '@source_name' as source_name, measurement_concept_id, count(distinct person_id) as num_persons, count(person_id) as num_records
  from cdm_schema.dbo.measurement
  where value_as_number > 0
  and measurement_concept_id > 0
  group by measurement_concept_id
  
) t1
group by measurement_concept_id
;


IF OBJECT_ID('tempdb..#measure_unit_van_summary', 'U') IS NOT NULL
drop table #measure_unit_van_summary;
create table #measure_unit_van_summary with (location=user_db, distribution=replicate) as
select measurement_concept_id, unit_concept_id, count(distinct source_name) as num_sources, sum(num_persons) as num_persons, sum(num_records) as num_records
from
(
  select '@source_name' as source_name, measurement_concept_id, unit_concept_id, count(distinct person_id) as num_persons, count(person_id) as num_records
  from cdm_schema.dbo.measurement
  where value_as_number > 0
  and measurement_concept_id > 0
  group by measurement_concept_id, unit_concept_id
  
) t1
group by measurement_concept_id, unit_concept_id
;



IF OBJECT_ID('tempdb..#measure_vaci_summary', 'U') IS NOT NULL
drop table #measure_vaci_summary;
create table #measure_vaci_summary with (location=user_db, distribution=replicate) as
select measurement_concept_id, count(distinct source_name) as num_sources, sum(num_persons) as num_persons, sum(num_records) as num_records
from
(
  select '@source_name' as source_name, measurement_concept_id, count(distinct person_id) as num_persons, count(person_id) as num_records
  from cdm_schema.dbo.measurement
  where value_as_concept_id > 0
  and measurement_concept_id > 0
  group by measurement_concept_id
  
) t1
group by measurement_concept_id
;


IF OBJECT_ID('tempdb..#measure_unit_vaci_summary', 'U') IS NOT NULL
drop table #measure_unit_vaci_summary;
create table #measure_unit_vaci_summary with (location=user_db, distribution=replicate) as
select measurement_concept_id, unit_concept_id, count(distinct source_name) as num_sources, sum(num_persons) as num_persons, sum(num_records) as num_records
from
(
  select '@source_name' as source_name, measurement_concept_id, unit_concept_id, count(distinct person_id) as num_persons, count(person_id) as num_records
  from cdm_schema.dbo.measurement
  where value_as_concept_id > 0
  and measurement_concept_id > 0
  group by measurement_concept_id, unit_concept_id
  
) t1
group by measurement_concept_id, unit_concept_id
;



IF OBJECT_ID('tempdb..#measure_summary_full', 'U') IS NOT NULL
drop table #measure_summary_full;
create table #measure_summary_full with (location=user_db, distribution=replicate) as
select c1.concept_id, c1.concept_name, c1.vocabulary_id,
ms1.num_sources, ms1.num_persons, ms1.num_records, 
mvs1.num_sources as num_sources_w_value_as_number, mvs1.num_persons as num_persons_w_value_as_number, mvs1.num_records as num_records_w_value_as_number, 
muvs1.num_units as num_units_w_value_as_number,
mvcs1.num_sources as num_sources_w_value_as_concept, mvcs1.num_persons as num_persons_w_value_as_concept, mvcs1.num_records as num_records_w_value_as_concept, 
muvcs1.num_units as num_units_w_value_as_concept,
1.0*mvs1.num_records / ms1.num_records as pct_records_w_value_as_number,
1.0*mvcs1.num_records / ms1.num_records as pct_records_w_value_as_concept
from
concept c1
inner join
#measure_summary ms1
on c1.concept_id = ms1.measurement_concept_id
left join
#measure_van_summary mvs1
on c1.concept_id = mvs1.measurement_concept_id
left join
(
  select measurement_concept_id, count(distinct unit_concept_id) as num_units
  from #measure_unit_van_summary
  group by measurement_concept_id
) muvs1
on c1.concept_id = muvs1.measurement_concept_id
left join
#measure_vaci_summary mvcs1
on c1.concept_id = mvcs1.measurement_concept_id
left join
(
  select measurement_concept_id, count(distinct unit_concept_id) as num_units
  from #measure_unit_vaci_summary
  group by measurement_concept_id
) muvcs1
on c1.concept_id = muvcs1.measurement_concept_id
where mvs1.num_records is not null or mvcs1.num_records is not null
;







select * from #measure_summary_full;




select c1.concept_name as measurement_concept_name,
c2.concept_name as unit_concept_name,
t1.*
  from
(
  select '@source_name' as source_name, 
  cast(stratum_1 as bigint) as measurement_concept_id,
  cast(stratum_2 as bigint) as unit_concept_id,
  count_value, min_value, max_value, avg_value, stdev_value, median_value, p10_value, p25_value, p75_value, p90_value
  from cdm_schema.dbo.achilles_results_dist
  where analysis_id = 1815
  
  
) t1
inner join concept c1
on t1.measurement_concept_id = c1.concept_id
inner join concept c2
on t1.unit_concept_id = c2.concept_id
where c1.concept_id > 0
order by c1.concept_id, c2.concept_id;  