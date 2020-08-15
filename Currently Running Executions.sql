--Currently running with query plans and query texts

--Currently running with query plans and query texts
SET TRANSACTION ISOLATION LEVEL READ UNCOMMITTED;
select s.session_id, DB_NAME(S.database_id), r.blocking_session_id, s.login_time, s.login_name, s.host_name, s.program_name, 
substring(qt.text, (r.statement_start_offset / 2) + 1, 
         ( (
         case r.statement_end_offset 
         when -1 then datalength(qt.text) 
         else r.statement_end_offset end - r.statement_start_offset) / 2 ) + 1 ) StatementText,
qt.text, qp.query_plan 
from sys.dm_exec_sessions s 
	inner join sys.dm_exec_requests r on s.session_id = r.session_id 
	cross apply sys.dm_exec_query_plan(r.plan_handle) qp 
outer apply sys.dm_exec_sql_text(r.sql_handle) qt

--CPU times for currently running query
select session_id, db_name(database_id), total_elapsed_time, cpu_time, reads, writes, logical_reads, dop
from sys.dm_exec_requests
where session_id = 269



--Long Running Transactions
select top 100 atr.transaction_id, atr.transaction_begin_time, 
from sys.dm_tran_active_transactions atr
inner join sys.dm_tran_session_transactions st on atr.transaction_id= st.transaction_id
inner join sys.dm_exec_requests r on st.session_id = r.session_id 
cross apply sys.dm_exec_query_plan(r.plan_handle) qp 
order by transaction_begin_time


select * from sys.dm_exec_session_wait_stats where session_id = 362 order by wait_time_ms desc


--Memory utilization information
declare @maxMemory int = (select cntr_value from sys.dm_os_performance_counters where counter_name like '%maximum workspace memory%' and object_name = 'SQLServer:Memory Manager')
select 
	mg.session_id
	, s.program_name
	, s.original_login_name
	, format(1.0 * requested_memory_kb / @maxMemory	, 'P') PercentOfServerMemoryRequested --This is the important metric. It's how much of SQL Server's memory is used.
	, format(1.0* max_used_memory_kb / requested_memory_kb, 'P') PercentOfAllocationUsedByQuery --This is showing how efficiently the granted memory is used. If the PercentOfServerMemoryRequested is high and this number is low, that's bad. It means that we're not using memory efficiently.
	, sqlt.text
	, requested_memory_kb / 1024 requested_memory_mb
	, mg.*
from sys.dm_exec_query_memory_grants mg
	inner join sys.dm_exec_sessions s on mg.session_id = s.session_id
	outer apply sys.dm_exec_sql_text(mg.sql_handle) sqlt
	--cross apply sys.dm_exec_query_plan(mg.plan_handle) ph --Commented out for performance, but you can add back in if you want to see query plans.
--where text like ' SELECT "MYC_MESG"."CREATED_TIME", "MYC_MESG"."TOFR%' --one possible way to narrow down the results if there are a lot of rows returned
order by requested_memory_kb desc


--Find size of session temp tables
select object_id, name from tempdb.sys.tables where name like '#<name of temp table>%'
select * from SYS.DM_DB_DATABASE_PAGE_ALLOCATIONS(2, -1417684813, NULL, NULL, 'detailed')
/*
@DatabaseId = 2
@TableId = -1417684813
@IndexId = 1 --clustered, probably doesn't matter for temp tables so just put NULL
@Mode = 'limited' --limited or detailed
*/


--Number of pending memory grants
select count(*) total
	, sum(case when grant_time is null then 1 else 0 end) pending
from sys.dm_exec_query_memory_grants mg


--SQL Agent Jobs Running
SELECT
    ja.job_id,
    j.name AS job_name,
    ja.start_execution_date,      
    ISNULL(last_executed_step_id,0)+1 AS current_executed_step_id,
    Js.step_name
FROM msdb.dbo.sysjobactivity ja 
LEFT JOIN msdb.dbo.sysjobhistory jh 
    ON ja.job_history_id = jh.instance_id
JOIN msdb.dbo.sysjobs j 
ON ja.job_id = j.job_id
JOIN msdb.dbo.sysjobsteps js
    ON ja.job_id = js.job_id
    AND ISNULL(ja.last_executed_step_id,0)+1 = js.step_id
WHERE ja.session_id = (SELECT TOP 1 session_id FROM msdb.dbo.syssessions ORDER BY agent_start_date DESC)
AND start_execution_date is not null
AND stop_execution_date is null;


--Query text for one process ID
DECLARE @sqltext VARBINARY(128)
SELECT @sqltext = sql_handle
FROM sys.sysprocesses
WHERE spid = 109
SELECT TEXT
FROM sys.dm_exec_sql_text(@sqltext)

DBCC INPUTBUFFER(109)
GO


