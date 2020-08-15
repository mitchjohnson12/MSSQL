--Size and duration of large tables
select cts.tableName, avg(datediff(MINUTE, startdate, enddate)) duration_s, sum(p.rows) totalRows,
	1.0 * SUM(a.total_pages) * 8 / 1024 / 1024 AS TotalSpaceGB
from master.dbo.CheckTableStatus cts
	left join sys.tables t on
		t.name = cts.tableName and
		cts.schemaName = schema_name(t.schema_id)
	left join sys.partitions p on 
		t.object_id = p.object_id 
		and p.index_id = 1
	left join sys.allocation_units a ON 
		p.partition_id = a.container_id
group by cts.tableName
order by TotalSpaceGB desc


--Duration of first CHECKTABLE run
drop table if exists #tmp
select *
into #tmp
from master.dbo.CheckTableStatus cts
where startdate = (select cast(min(cast(startdate as float)) as datetime) 
					from master.dbo.checktablestatus 
					where tablename = cts.tablename and schemaname = cts.schemaname)

select top 100 
	cast(min(cast(startdate as float)) as datetime) jobStart
	, cast(max(cast(enddate as float)) as datetime) jobEnd
	, sum(datediff(ms, startdate, enddate))/1000/60 totalDuration_minutes
from #tmp




select count(*) from master.dbo.CheckTableStatus
 --and tablename like 'pat%'
--where procflag =0


--size of remaining tables
select cts.tableName, avg(datediff(ss, startdate, enddate)) duration_s, sum(p.rows) totalRows,
	1.0 * SUM(a.total_pages) * 8 / 1024 / 1024 AS TotalSpaceGB
from master.dbo.CheckTableStatus cts
	left join sys.tables t on
		t.name = cts.tableName and
		cts.schemaName = schema_name(t.schema_id)
	left join sys.partitions p on 
		t.object_id = p.object_id 
		and p.index_id = 1
	left join sys.allocation_units a ON 
		p.partition_id = a.container_id
--where cts.procflag = 
group by cts.tableName
order by TotalSpaceGB desc


--size and rows per day
select cts.procFlag, cast(cts.startdate as date) startDate, count(*) numTables
	, 1.0 * SUM(a.total_pages) * 8 / 1024 / 1024 TotalSpaceGB
	, sum(cast(p.rows as bigint)) totalRows
	, cast(cast(max(cast(endDate as float)) as datetime) as time) endTime
	, cast(cast(max(cast(enddate as float)) - min(cast(startDate as float)) as datetime) as time) totalTime
from master.dbo.CheckTableStatus cts
	left join sys.tables t on
		t.name = cts.tableName and
		cts.schemaName = schema_name(t.schema_id)
	left join sys.partitions p on 
		t.object_id = p.object_id 
		and p.index_id = 1
	left join sys.allocation_units a ON 
		p.partition_id = a.container_id
group by procFlag, cast(startdate as date)
order by startdate



--size of really large tables
select t.name
	, 1.0 * SUM(a.total_pages) * 8 / 1024 / 1024 TotalSpaceGB
	, sum(p.rows ) totalRows
from sys.tables t
	left join sys.partitions p on 
		t.object_id = p.object_id 
		and p.index_id = 1
	left join sys.allocation_units a ON 
		p.partition_id = a.container_id
where t.name in ('ACC_LOG_DTL_IX', 'ACCESS_LOG', 'ACC_LOG_DTL_NI', 'ACC_LOG_MTLDTL_IX', 'ACC_LOG_MTLDTL_NI')
group by t.name



--first start time
select top 100 cast(startdate as date) dt, cast(min(cast(startdate as float)) as datetime) firstTableStart
from master.dbo.CheckTableStatus
where startdate is not null
group by cast(startdate as date)
order by dt
