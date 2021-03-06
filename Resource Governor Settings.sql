declare @maxMemory int = (select cntr_value from sys.dm_os_performance_counters where counter_name like '%maximum workspace memory%' and object_name = 'SQLServer:Memory Manager')
select top 100 
	execution_count 'Number of times this query has been run'
	, max_grant_kb / 1024/1024 'max granted memory (GB)'
	, format(1.0 * max_grant_kb / @maxMemory, 'P')  'Percent of total memory'
	, max_used_grant_kb / 1024/1024 'max used memory (GB)'
	, format(1.0 * max_used_grant_kb / @maxMemory, 'P')  'Percent of total memory'
from sys.dm_exec_query_stats
order by [max used memory (GB)] desc

select top 1 * from sys.dm_exec_query_stats



select top 1 
eqs.max_ideal_grant_kb
, * --I used select top 100 here for the sake of performance on my own machine
,SUBSTRING(ST.text, (eqs.statement_start_offset/2) + 1,
    ((CASE statement_end_offset  
        WHEN -1 THEN DATALENGTH(ST.text)
        ELSE eqs.statement_end_offset END  
            - eqs.statement_start_offset)/2) + 1) AS statement_text
, cast(qp.query_plan as xml) statement_query_plan
from sys.dm_exec_query_stats eqs
	cross apply
	sys.dm_exec_sql_text(eqs.sql_handle) st
	cross apply
	sys.dm_exec_text_query_plan(eqs.plan_handle, eqs.statement_start_offset, eqs.statement_end_offset) qp
	cross apply
	sys.dm_exec_query_plan(eqs.plan_handle) qp2


