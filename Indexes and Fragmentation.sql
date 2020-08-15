/*
Truncate table [master].[dbo].[Todo_index_tables]
INSERT INTO [master].[dbo].[Todo_index_tables]
VALUES ('ORDER_MED'), ('ORDER_PROC'), ('PAT_ENC_HSP'),('F_IP_HSP_TRANSFER'),('F_IP_HSP_PAT_DAYS'),('HSP_ACCOUNT'),('IP_FLWSHT_MEAS'),('ORDER_RESULTS'),('PAT_ENC_HOSP_PROB')
*/

---------------------------------------------------------
--Run in the morning to see where yesterday's focus was--
---------------------------------------------------------
select i.[table], i.[index], i.[avg_fragmentation_in_percent], i.page_count, cl.command, cl.starttime, cl.endtime
from master.dbo.Index_frag i
	inner join master.dbo.commandlog cl on cl.indexname = i.[index] 
	and cl.starttime >  '2019-04-24'
	and cl.commandtype = 'alter_index'
	order by starttime desc


select top 100
	format(sum(case when avg_fragmentation_in_percent <= 25 then page_count else 0 end),'N0') NumPages5to25
	, format(sum(case when avg_fragmentation_in_percent between 25 and 50 then page_count else 0 end),'N0') NumPages25to50 
	, format(sum(case when avg_fragmentation_in_percent >= 50 then page_count else 0 end),'N0') NumPages50to100
	, format(sum(case when avg_fragmentation_in_percent <= 25 then 1 else 0 end),'N0') NumIndexes5to25
	, format(sum(case when avg_fragmentation_in_percent between 25 and 50 then 1 else 0 end),'N0') NumIndexes25to50 
	, format(sum(case when avg_fragmentation_in_percent >= 50 then 1 else 0 end),'N0') NumIndexes50to100
from master.dbo.index_frag
where avg_fragmentation_in_percent > 5
	and [schema] = 'dbo'

select top 100 * from master.dbo.todo_index_tables

--Index Frag with Last Rebuild for one derived table
use clarity;
declare @mainDerivedTable varchar(30)= 'F_IP_HSP_PAT_DAYS'
select top 100 
	frag.[table]
	,frag.[index]
	,frag.avg_fragmentation_in_percent
	,frag.page_count
	,case when cl.command like '%reorganize with%' then 'Y' else 'N' end Reorg_YN
	,case when cl.command like '%rebuild with%' then 'Y' else 'N' end Rebuild_YN
	,datediff(DAY, cl.StartTime, getdate()) DaysSinceLastIndexMaintenance
from master.dbo.index_frag frag
	inner join (
		select distinct 
			dep.dep_column_table
		from clarity_tbl tbl
			inner join clarity_tbl_2 tbl2 on
				tbl.table_id = tbl2.table_id
			inner join db_obj_basic_info b on
				tbl2.database_object_id = b.record_id
			inner join db_obj_dep_cols dep ON
				b.record_id = dep.record_id
		where tbl.table_name = @mainDerivedTable
		UNION ALL
		select @mainDerivedTable
				) dertblDependencies
			on dertblDependencies.dep_column_table = frag.[table]
	left join (	select c.*
				from master.dbo.commandlog c
					inner join (select objectname, indexname, max(StartTime) startTime from master.dbo.commandlog where commandtype = 'alter_index' and errormessage is null group by ObjectName, indexname) mx
						on mx.ObjectName = c.ObjectName and mx.IndexName = c.IndexName and mx.startTime = c.StartTime
				where errormessage is null 
					and c.Commandtype = 'ALTER_INDEX'
					--and c.indexname like 'pk%'
					) cl on
		frag.[table] = cl.ObjectName
		and frag.[index] = 	cl.IndexName
	--left join clarity_tbl_ix ix on
	--	ix.table_id = tbl.table_id and frag.[index] = ix.INDEX_NAME
--where
	--and frag.[index] like 'pk%'
order by frag.[table], avg_fragmentation_in_percent desc


---------------------------------------------------
--Index Frag with Last Rebuild for list of tables--
---------------------------------------------------

use clarity;
select top 100 
	frag.[table]
	,frag.[index]
	,frag.avg_fragmentation_in_percent
	,frag.page_count
	,case when cl.command like '%reorganize with%' then 'Y' else 'N' end Reorg_YN
	,case when cl.command like '%rebuild with%' then 'Y' else 'N' end Rebuild_YN
	,datediff(DAY, cl.StartTime, getdate()) DaysSinceLastIndexMaintenance
from master.dbo.index_frag frag
	left join (	select c.*
				from master.dbo.commandlog c
					inner join (select objectname, indexname, max(StartTime) startTime from master.dbo.commandlog where commandtype = 'alter_index' and errormessage is null group by ObjectName, indexname) mx
						on mx.ObjectName = c.ObjectName and mx.IndexName = c.IndexName and mx.startTime = c.StartTime
				where errormessage is null 
					and c.Commandtype = 'ALTER_INDEX'
					--and c.indexname like 'pk%'
					) cl on
		frag.[table] = cl.ObjectName
		and frag.[index] = 	cl.IndexName
	--left join clarity_tbl_ix ix on
	--	ix.table_id = tbl.table_id and frag.[index] = ix.INDEX_NAME
where
	frag.[table] in ('HSP_TRANSACTIONS','ORDER_PROC','PAT_ENC','HSP_CLP_REV_CODE')
	--and frag.[index] like 'pk%'
	and frag.avg_fragmentation_in_percent > 5
order by frag.[table], avg_fragmentation_in_percent desc






--'ORDER_MED', 'ORDER_PROC', 'PAT_ENC_HSP','F_IP_HSP_TRANSFE','F_IP_HSP_PAT_DAYS','HSP_ACCOUNT','IP_FLWSHT_MEAS','ORDER_RESULTS','PAT_ENC_HOSP_PROB'

--which indexes are used
declare @mainDerivedTable varchar(30) = 'F_IP_HSP_PAT_DAYS'
select distinct 
	tbl.table_name
	, dep.*
from clarity_tbl tbl
	inner join clarity_tbl_2 tbl2 on
		tbl.table_id = tbl2.table_id
	inner join db_obj_basic_info b on
		tbl2.database_object_id = b.record_id
	inner join db_obj_dep_cols dep ON
		b.record_id = dep.record_id
where tbl.table_name = @mainDerivedTable

--History of index maintenance
declare @startDate date =EPIC_UTIL.EFN_DIN('t-31');
select top 100 
	ObjectName
	, IndexName
	, CommandType
	, count(*)
	, min(StartTime) StartTimeMin
	, max(StartTime) StartTimeMax
	, DATEDIFF(minute, min(StartTime), max(StartTime))
from master.dbo.commandlog
where objectname is not null
	and startTime > @startDate
	and ObjectName in ('F_IP_HSP_TRANSFER', 'CLARITY_ADT',	'CL_ORD_FST_LST_SCH',	'ED_IEV_EVENT_INFO',	'ED_IEV_PAT_INFO',	'HSP_ATND_PROV',	'IP_ORDER_REC',	'ORDER_LAST_EDIT',	'ORDER_MED',	'ORDER_PROC',	'PAT_ENC_HSP')
	and Commandtype = 'ALTER_INDEX'
	and errorMessage is null
group by ObjectName, IndexName, commandtype
order by ObjectName, indexName, CommandType

--Find derived tables with recent full extracts
use clarity;
select top 100 * from cr_stat_dertbl
where load_type = 'full'
order by ckpt_init desc

select top 100 frag.*, tbl.load_type
from master.dbo.index_frag frag
	inner join clarity_tbl tbl on
		tbl.table_name = frag.[table]
where [table] like 'F%' 
	and page_count > 100
	and load_type = 'full'

--How much are we processing per day
select
	cast(starttime as date) dt
	, min(starttime) FirstIndexStart
	, max(endtime) LastIndexEnd
	, count(*) NumIndexes
	, format(datediff(MINUTE, min(starttime), max(endtime)) / 60.0, 'N', 'en-us') totalHours
	, sum(i.page_count) page_count
from master.dbo.commandlog cl
	inner join master.dbo.Index_frag i on
		cl.indexname = i.[index]
where commandtype = 'alter_index'
	and starttime > '2019-01-01'
group by cast(starttime as date)
order by dt desc

select top 1 * from master.dbo.index_frag
select top 1 * from master.dbo.commandlog

select i.[table], i.[index], i.[avg_fragmentation_in_percent], i.page_count, cl.command, cl.starttime, cl.endtime
from master.dbo.Index_frag i
	inner join master.dbo.commandlog cl on cl.indexname = i.[index] 
	and cl.starttime >  '2019-04-10'
	and cl.commandtype = 'alter_index'
	and command like '%rebuild%'

select top 100 * from master.dbo.Todo_index_tables




