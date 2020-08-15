
-- Details About Statistics
SELECT DISTINCT
	OBJECT_NAME(s.[object_id]) AS TableName,
	c.name AS ColumnName,
	s.name AS StatName,
	s.auto_created,
	s.user_created,
	s.no_recompute,
	s.[object_id],
	s.stats_id,
	sc.stats_column_id,
	sc.column_id,
	STATS_DATE(s.[object_id], s.stats_id) AS LastUpdated
FROM sys.stats s 
JOIN sys.stats_columns sc ON sc.[object_id] = s.[object_id] AND sc.stats_id = s.stats_id
--JOIN sys.tables t on s.[object_id] = t.[object_id]
JOIN sys.columns c ON c.[object_id] = sc.[object_id] AND c.column_id = sc.column_id
JOIN sys.partitions par ON par.[object_id] = s.[object_id]
JOIN sys.objects obj ON par.[object_id] = obj.[object_id]
WHERE OBJECTPROPERTY(s.OBJECT_ID,'IsUserTable') = 1
	and object_name(s.object_id) = 'D_MU_OBJ_MEASURES'
AND (s.auto_created = 1 OR s.user_created = 1)
--and 	STATS_DATE(s.[object_id], s.stats_id) is null
order by columnname
