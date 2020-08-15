--Finding plans from the Query Store
/*
------
--Grab the query IDs and plug them into the second query
------
select distinct qt.query_sql_text, concat(query_text_id, ',')
from sys.query_store_query_text qt
where qt.query_sql_text like 'with --eap_dt as (select proc_id, max(contact_date)%'
*/

-----
--View performance info
-----

drop table if exists #tmp
select top 10000 (len(query_plan)) plan_chars
into #tmp
from sys.query_store_plan

select *
from #tmp
order by plan_chars desc


use clarity;
select  
	format(avg_duration / power(10,6), 'N0') avg_duration_s
	, LEFT(RIGHT(qt.query_sql_text, LEN(qt.query_sql_text) - charindex('and tdl.post_date <= {d', qt.query_sql_text) -20), 16) search_date
	, qt.query_text_id
	--, cast(p.query_plan as xml)
	, p.query_plan 
	, len(p.query_plan) query_plan_character_length
	, p.query_plan
	, qt.query_sql_text
	, rs.*
from sys.query_store_query_text qt
inner join sys.query_store_query q on qt.query_text_id = q.query_text_id
inner join sys.query_store_plan p on p.query_id = q.query_id
inner join sys.query_store_runtime_stats rs on rs.plan_id = p.plan_id
where qt.query_text like 'ACCESS_LOG'
	--and qt.query_text_id in (22568963,24341073,24620140,24514294,24514292,24514293,24539554)
	--and execution_type != 3 --exclude aborted executions
	--and p.plan_id not in (25518670, 25491302, 25491303)
order by execution_type_desc--, plan_id