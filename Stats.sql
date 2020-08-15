--Stats on large, frequently used tables
use clarity;
with dmca as (
select [OBJECT_NAME] tablename
	, COUNT(*) cnt
from CR_DMCA_DEPENDENCY
WHERE OBJECT_TYPE = 'TABLE'
GROUP BY [OBJECT_NAME]
)
select dmca.tablename
	, dmca.cnt
	, t.max_column_id_used
	, stat.name
	, sp.rows
	, sp.rows_sampled
	, format(1.0 * sp.rows_sampled / sp.rows, 'P') SampleRate
	, sp.last_updated
from dmca 
	inner join sys.tables t on t.name = dmca.tablename 
	left join sys.stats stat on stat.object_id = t.object_id
	cross apply sys.dm_db_stats_properties(stat.object_id, stat.stats_id) AS sp
where stat.stats_id = 1
	and dmca.cnt > 100
	--and sp.index_id = 1
	and sp.rows > 10000000
order by dmca.cnt desc




--Highly modified
use clarity;
SELECT obj.name, obj.object_id, stat.name, stat.stats_id, last_updated, modification_counter  
FROM sys.objects AS obj   
INNER JOIN sys.stats AS stat ON stat.object_id = obj.object_id  
CROSS APPLY sys.dm_db_stats_properties(stat.object_id, stat.stats_id) AS sp  
WHERE modification_counter > 1000 
	and obj.name = 'pat_enc'

--List of tables
use clarity;
SELECT sp.stats_id, stat.object_id, stat.name, filter_definition, last_updated, rows, rows_sampled, steps, unfiltered_rows, modification_counter, format(1.0 * sp.rows_sampled/rows, 'P')
FROM sys.stats AS stat   
CROSS APPLY sys.dm_db_stats_properties(stat.object_id, stat.stats_id) AS sp  
left join sys.tables t on t.object_id = stat.object_id
WHERE t.name in ('PAT_ENC',	'F_SCHED_APPT',	'CLARITY_DEP',	'CLARITY_SER_2',	'ZC_LICENSE_DISPLAY',	'ED_IEV_PAT_INFO',	'ED_IEV_EVENT_INFO',	'ED_IEV_EVENT_INFO',	'ED_IEV_EVENT_INFO',	'ED_IEV_EVENT_INFO',	'ED_IEV_EVENT_INFO',	'ED_IEV_EVENT_INFO',	'PATIENT',	'PATIENT_3',	'CLARITY_LOC',	'ZC_DEP_RPT_GRP_6',	'ZC_DEP_RPT_GRP_10')


select top 100 *
from sys.tables t 
	inner join sys.partitions p on t.object_id = p.object_id 
WHERE t.name = 'patient'
	and p.index_id = 1



use clarity;
SELECT sp.stats_id, stat.object_id, stat.name, filter_definition, last_updated, rows, rows_sampled, steps, unfiltered_rows, modification_counter, format(1.0 * rows_sampled/rows, 'P')
FROM sys.stats AS stat   
CROSS APPLY sys.dm_db_stats_properties(stat.object_id, stat.stats_id) AS sp  
left join sys.tables t on t.object_id = stat.object_id
WHERE t.name in ('PAT_ENC',	'F_SCHED_APPT',	'CLARITY_DEP',	'CLARITY_SER_2',	'ZC_LICENSE_DISPLAY',	'ED_IEV_PAT_INFO',	'ED_IEV_EVENT_INFO',	'ED_IEV_EVENT_INFO',	'ED_IEV_EVENT_INFO',	'ED_IEV_EVENT_INFO',	'ED_IEV_EVENT_INFO',	'ED_IEV_EVENT_INFO',	'PATIENT',	'PATIENT_3',	'CLARITY_LOC',	'ZC_DEP_RPT_GRP_6',	'ZC_DEP_RPT_GRP_10')



SELECT *-- sp.stats_id, stat.object_id, stat.name, filter_definition, last_updated, rows, rows_sampled, steps, unfiltered_rows, modification_counter, format(1.0 * rows_sampled/rows, 'P')
FROM sys.stats AS stat   
CROSS APPLY sys.dm_db_stats_properties(stat.object_id, stat.stats_id) AS sp  
left join sys.tables t on t.object_id = stat.object_id
WHERE t.name = 'ED_IEV_PAT_INFO'

--Attempt 1
use clarity;
with dmca as (
select [OBJECT_NAME] tablename
	, COUNT(*) cnt
from CR_DMCA_DEPENDENCY
WHERE OBJECT_TYPE = 'TABLE'
GROUP BY [OBJECT_NAME]
)
, rowcnt as (
select dmca.*, t.object_id, p.rows
from dmca
	INNER JOIN sys.tables t ON t.name = dmca.tablename
	inner join sys.partitions p on t.object_id = p.object_id 
where dmca.cnt > 100
	and p.index_id = 1
	and p.rows > 10000000
)
select *
from rowcnt rc
	left join sys.stats stat on stat.object_id = rc.object_id
	cross apply sys.dm_db_stats_properties(stat.object_id, stat.stats_id) AS sp
where stat.stats_id = 1
order by rc.cnt desc
