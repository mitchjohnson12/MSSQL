--Source: https://www.mssqltips.com/sqlservertip/3485/sql-server-dbcc-checkdb-checkcatalog-and-checkalloc-for-vldbs/

USE Admin;
GO

IF OBJECT_ID('dbo.CustomDBCC', 'P') IS NULL
 EXEC('CREATE PROCEDURE [dbo].[CustomDBCC] AS SELECT 0');
GO

/********************************************************************************************************************
*Author: Mike Eastland                                *
*                                         *
*Notes:  The purpose of this stored procedure is to run one or more DBCC commands as dictated by the parameters *
*    passed at run-time. It has been designed to accommodate VLDBs. It is recommended to create this  *
*    procedure in a dedicated administrative database rather than a system or application database.  *
********************************************************************************************************************/
ALTER PROCEDURE [dbo].[CustomDBCC] (
 @checkAlloc  BIT = 0,     -- Execute DBCC CHECKALLOC
 @checkCat  BIT = 0,     -- Execute DBCC CHECKCATALOG
 @checkDB  BIT = 1,     -- Execute DBCC CHECKDB (which includes CHECKALLOC and CHECKCATALOG)
 @checkNdx  BIT = 1,     -- Include indexes in DBCC commands
 @dbName   SYSNAME = NULL,    -- Run for a single database
 @dbExcludeList VARCHAR(MAX) = NULL,  -- Comma-separated list of databases to exclude
 @debugMode  BIT = 0,     -- Prevent execution of DBCC commands (@debugMode = 1)
 @maxDuration INT = 0,     -- Number of hours the procedure is allowed to run (0 = to completion)
 @physOnly  BIT = 0,     -- Run CHECKDB with PHYSICAL_ONLY option
 @tableName  SYSNAME = NULL,    -- Run for a single table
 @tblExcludeList VARCHAR(MAX) = NULL,  -- Comma-separated list of tables to exclude
 @vldbMode  BIT = 0      -- Execute DBCC commands at the table-level for VLDBs
)
AS

SET NOCOUNT, XACT_ABORT ON

DECLARE @db   VARCHAR(128),
  @dbclause VARCHAR(128),
  @end  DATETIME,
  @msg  VARCHAR(1024),
  @restart BIT,
  @sql  NVARCHAR(MAX),
  @tbl  VARCHAR(128),
  @tblid  INT;

DECLARE @db_tbl  TABLE ( DatabaseName VARCHAR(128), ProcFlag  BIT DEFAULT(0) ); --ProcFlag (0 = CHECKTABLE has not yet been performed ; 1 = CHECKTABLE complete; NULL = table skipped per @tblExcludeList or @debugMode)

DECLARE @check_tbl TABLE ( DatabaseName VARCHAR(128),
       SchemaName  VARCHAR(128),
       TableName  VARCHAR(128) );

SET @msg = 'DBCC job on ' + @@SERVERNAME + ' started at ' + CONVERT(VARCHAR, GETDATE()) + '.' + CHAR(10) + CHAR(13);
 RAISERROR(@msg, 0, 0) WITH NOWAIT;

-- Set initial / default variable values
SELECT @vldbMode = ISNULL(@vldbMode, 0), @physOnly = ISNULL(@physOnly, 0), @restart = 1,
  @maxDuration = CASE WHEN @maxDuration IS NULL THEN 0 ELSE @maxDuration END,
  @dbName = CASE LTRIM(RTRIM(@dbName)) WHEN '' THEN NULL ELSE LTRIM(RTRIM(@dbName)) END,
  @dbExcludeList = CASE ISNULL(@dbName, 'NULL') WHEN 'NULL' THEN @dbExcludeList ELSE NULL END;

SELECT @end = CASE @maxDuration WHEN 0 THEN '9999-12-31 23:59:59:997' ELSE DATEADD(MINUTE, @maxDuration * 60, GETDATE()) END,
  @checkDB = CASE @vldbMode WHEN 0 THEN @checkDB ELSE 0 END,
  @checkCat = CASE @checkDB WHEN 1 THEN 0 ELSE @checkCat END,
  @checkAlloc = CASE @checkDB WHEN 1 THEN 0 ELSE @checkAlloc END;

-- Validate variables
IF @dbName IS NOT NULL AND DB_ID(@dbName) IS NULL
BEGIN
 SET @msg = 'Database {' + @dbName + '} does not exist. Procedure aborted at ' + CONVERT(VARCHAR, GETDATE()) + '.';
  RAISERROR(@msg, 0, 0) WITH NOWAIT;
   RETURN;
END
ELSE
BEGIN
 SET @msg = 'DBCC job will execute for a single database {' + @dbName + '}';
  RAISERROR(@msg, 0, 0) WITH NOWAIT;
END

IF @tableName IS NOT NULL
BEGIN
 IF @vldbMode <> 1
 BEGIN
  SET @msg = 'The @vldbMode parameter must be set if @tableName is not null. Procedure aborted at ' + CONVERT(VARCHAR, GETDATE()) + '.';
   RAISERROR(@msg, 0, 0) WITH NOWAIT;
    RETURN;
 END
 ELSE
 BEGIN
  SET @msg = 'DBCC job will execute for a single table {' + @tableName + '} in each target database.';
   RAISERROR(@msg, 0, 0) WITH NOWAIT;
 END
END

IF @tblExcludeList IS NOT NULL AND @vldbMode <> 1
BEGIN
 SET @msg = 'The @vldbMode parameter must be set if @tblExcludeList is not null. Procedure aborted at ' + CONVERT(VARCHAR, GETDATE()) + '.';
  RAISERROR(@msg, 0, 0) WITH NOWAIT;
   RETURN;
END

IF @checkAlloc = 0 AND @checkCat = 0 AND @checkDB = 0 AND @vldbMode = 0
BEGIN
 SET @msg = 'Invalid parameter combination would result in no DBCC commands executed. Procedure aborted at ' + CONVERT(VARCHAR, GETDATE()) + '.';
  RAISERROR(@msg, 0, 0) WITH NOWAIT;
   RETURN;
END

IF @debugMode = 1
BEGIN
 SET @msg = 'Procedure [' + OBJECT_NAME(@@PROCID) + '] is running in debug mode. No integrity check commands will be executed.';
  RAISERROR(@msg, 0, 0) WITH NOWAIT;
END

INSERT INTO @db_tbl (DatabaseName)
SELECT [name]
FROM [master].sys.databases
WHERE [source_database_id] IS NULL
AND [database_id] <> 2
AND DATABASEPROPERTYEX([name], 'Status') = 'ONLINE'
AND LOWER([name]) = LOWER(ISNULL(@dbName, [name]));

-- Exlude databases
IF (@dbExcludeList IS NOT NULL AND LTRIM(RTRIM(@dbExcludeList)) <> '')
BEGIN
 IF OBJECT_ID('dbo.CommaStringTable') IS NULL
 BEGIN
  SET @msg = 'The function required by skip-database code does not exist.  All databases will be checked.';
   RAISERROR(@msg, 0, 0) WITH NOWAIT;
 END
 ELSE
 BEGIN
  SET @msg = 'The following databases will be skipped: (' + LTRIM(RTRIM(@dbExcludeList)) + ').';
   RAISERROR(@msg, 0, 0) WITH NOWAIT;
  
  DELETE d
  FROM @db_tbl d
   INNER JOIN dbo.CommaStringTable(@dbExcludeList) f ON LOWER(d.DatabaseName) = LOWER(f.[Value]);
 END
END

IF NOT EXISTS ( SELECT * FROM @db_tbl WHERE ProcFlag = 0 )
BEGIN
 SET @msg = 'No databases match the supplied parameters. Procedure aborted at ' + CONVERT(VARCHAR, GETDATE()) + '.';
  RAISERROR(@msg, 0, 0) WITH NOWAIT;
   RETURN;
END

--
--Begin While Loop 1
--
WHILE EXISTS ( SELECT * FROM @db_tbl WHERE ProcFlag = 0 )
BEGIN
 SELECT TOP 1 @db = DatabaseName FROM @db_tbl WHERE ProcFlag = 0 ORDER BY DatabaseName;

 SET @dbclause = '[' + @db + CASE @checkNdx WHEN 1 THEN ']' ELSE '], NOINDEX' END;

 -- Execute database-level DBCC commands
 BEGIN TRY
  IF @checkAlloc = 1
  BEGIN
   SET @msg = 'DBCC CHECKALLOC against ' + QUOTENAME(@db) + ' started at ' + CONVERT(VARCHAR, GETDATE()) + '.';
    RAISERROR(@msg, 0, 0) WITH NOWAIT;

   SET @sql = 'SET QUOTED_IDENTIFIER OFF SET ARITHABORT ON DBCC CHECKALLOC (' + @dbclause + ') WITH ALL_ERRORMSGS, NO_INFOMSGS';
    RAISERROR(@sql, 0, 0) WITH NOWAIT;

   IF @debugMode = 0
    EXEC sp_ExecuteSQL @sql;

   SET @msg = 'DBCC CHECKALLOC against ' + QUOTENAME(@db) + ' completed at ' + CONVERT(VARCHAR, GETDATE()) + '.';
    RAISERROR(@msg, 0, 0) WITH NOWAIT;
  END
  
  IF @checkCat = 1
  BEGIN
   SET @msg = 'DBCC CATALOG against ' + QUOTENAME(@db) + ' started at ' + CONVERT(VARCHAR, GETDATE()) + '.'; --Update to "DBCC CHECKCATALOG against..."?
    RAISERROR(@msg, 0, 0) WITH NOWAIT;

   SET @sql = 'SET QUOTED_IDENTIFIER OFF SET ARITHABORT ON DBCC CHECKCATALOG ([' + @db + ']) WITH NO_INFOMSGS';
    RAISERROR(@sql, 0, 0) WITH NOWAIT;

   IF @debugMode = 0
    EXEC sp_ExecuteSQL @sql;

   SET @msg = 'DBCC CATALOG against ' + QUOTENAME(@db) + ' completed at ' + CONVERT(VARCHAR, GETDATE()) + '.';
    RAISERROR(@msg, 0, 0) WITH NOWAIT;
  END

  IF @checkDB = 1
  BEGIN
   SET @msg = 'DBCC CHECKDB against ' + QUOTENAME(@db) + ' started at ' + CONVERT(VARCHAR, GETDATE()) + '.';
    RAISERROR(@msg, 0, 0) WITH NOWAIT;

   SET @sql = 'SET QUOTED_IDENTIFIER OFF SET ARITHABORT ON DBCC CHECKDB (' + @dbclause + ') WITH ALL_ERRORMSGS, NO_INFOMSGS' + 
      CASE @physOnly WHEN 1 THEN ', PHYSICAL_ONLY' ELSE '' END;
    RAISERROR(@sql, 0, 0) WITH NOWAIT;

   IF @debugMode = 0
    EXEC sp_ExecuteSQL @sql;

   SET @msg = 'DBCC CHECKDB against ' + QUOTENAME(@db) + ' completed at ' + CONVERT(VARCHAR, GETDATE()) + '.';
    RAISERROR(@msg, 0, 0) WITH NOWAIT;
  END

  IF @vldbMode = 1
  BEGIN
   SET @sql = 'SELECT [TABLE_CATALOG], [TABLE_SCHEMA], [TABLE_NAME] FROM [' + @db + 
      '].[INFORMATION_SCHEMA].[TABLES] WHERE [TABLE_TYPE] = ''BASE TABLE'' ORDER BY [TABLE_NAME]';

   INSERT INTO @check_tbl ([DatabaseName], [SchemaName], [TableName])
   EXEC sp_ExecuteSQL @sql;
  END

  UPDATE @db_tbl SET ProcFlag = 1 WHERE DatabaseName = @db;

  IF @end < GETDATE()
  BEGIN
   SET @msg = 'Procedure has exceeded max run time based on @maxDuration parameter and will exit at ' + CONVERT(VARCHAR, GETDATE()) + '.';
    RAISERROR(@msg, 0, 0) WITH NOWAIT;
     RETURN;
  END
 END TRY
 
 BEGIN CATCH
  SET @msg = 'Failed to execute command {' + @sql + '} against database {' + @db + '} with error number: ' + CAST(ERROR_NUMBER() AS VARCHAR) + 
     '; error message: ' + ERROR_MESSAGE() + '.  Procedure terminated at ' + CONVERT(VARCHAR, GETDATE()) + '.';
   RAISERROR(@msg, 16, 1) WITH LOG, NOWAIT;
    RETURN(-1);  
 END CATCH
END

--
--End While Loop 1
--


IF @vldbMode = 1
BEGIN
 IF OBJECT_ID('[dbo].[CheckTableStatus]', 'U') IS NULL
  CREATE TABLE [dbo].[CheckTableStatus] ( [checkTableID] [BIGINT] IDENTITY(1,1) NOT NULL,
            [databaseName] [NVARCHAR](128) NOT NULL,
            [schemaName] [NVARCHAR](128) NOT NULL,
            [tableName]  [NVARCHAR](128) NOT NULL,
            [procFlag]  [BIT] NULL,
            [startDate]  [DATETIME] NULL,
            [endDate]  [DATETIME] NULL );
 ELSE
  DELETE FROM [dbo].[CheckTableStatus] WHERE [endDate] < GETDATE() - 367 AND ISNULL([procFlag], 1) = 1; 

 -- Check for outstanding CHECKTABLE commands
 IF EXISTS ( SELECT * FROM [dbo].[CheckTableStatus] WHERE [procFlag] = 0 )
  SET @restart = 0;

 IF @restart = 1
  INSERT INTO [dbo].[CheckTableStatus] ([databaseName], [schemaName], [tableName], [procFlag])
  SELECT DatabaseName, SchemaName, TableName, 0
  FROM @check_tbl c
  WHERE NOT EXISTS ( SELECT *
       FROM dbo.CommaStringTable(@tblExcludeList) f
       WHERE LOWER(f.[Value]) = LOWER(c.tableName) )
  AND LOWER(c.tableName) = LOWER(ISNULL(@tableName, c.tableName));
 ELSE
 BEGIN
  SET @msg = 'Procedure has unfinished business in VLDB mode.';
   RAISERROR(@msg, 0, 0) WITH NOWAIT;
 END

 -- Exclude tables
 IF (@tblExcludeList IS NOT NULL AND LTRIM(RTRIM(@tblExcludeList)) <> '')
 BEGIN
  IF OBJECT_ID('dbo.CommaStringTable') IS NULL
  BEGIN
   SELECT @msg = 'The function required by skip-table code does not exist. All tables will be checked.', @tblExcludeList = NULL;
    RAISERROR(@msg, 0, 0) WITH NOWAIT;
  END
  ELSE
  BEGIN
   SET @msg = 'The following tables will be skipped for all databases: (' + REPLACE(@tblExcludeList, ' ', '') + ').';
    RAISERROR(@msg, 0, 0) WITH NOWAIT;

   UPDATE cts
   SET cts.[procFlag] = NULL
   FROM [dbo].[CheckTableStatus] cts
    INNER JOIN dbo.CommaStringTable(@tblExcludeList) cst ON LOWER(cts.tableName) = LOWER(cst.[Value])
   WHERE ISNULL(cts.[procFlag], 0) = 0;
  END
 END

 --
 --Begin While Loop 2
 --
 WHILE EXISTS ( SELECT c.*
     FROM [dbo].[CheckTableStatus] c
      INNER JOIN @db_tbl t ON c.databaseName = t.DatabaseName
     WHERE c.procFlag = 0
     AND LOWER(c.tableName) = LOWER(ISNULL(@tableName, c.tableName)) )
 BEGIN  
  SELECT TOP 1 @tbl = '[' + c.databaseName + '].[' + c.schemaName + '].[' + c.tableName + ']', 
      @sql = 'SET QUOTED_IDENTIFIER OFF SET ARITHABORT ON DBCC CHECKTABLE (' + CHAR(39) + @tbl + CHAR(39) + 
      CASE @checkNdx WHEN 0 THEN ', NOINDEX' ELSE '' END + ') WITH ALL_ERRORMSGS, NO_INFOMSGS' + 
      CASE @physOnly WHEN 1 THEN ', PHYSICAL_ONLY' ELSE '' END, @tblid = c.checkTableID
  FROM [dbo].[CheckTableStatus] c
   INNER JOIN @db_tbl t ON c.databaseName = t.DatabaseName
  WHERE c.procFlag = 0
  AND LOWER(c.tableName) NOT IN ( SELECT LOWER([Value]) FROM dbo.CommaStringTable(@tblExcludeList) )
  AND LOWER(c.tableName) = LOWER(ISNULL(@tableName, c.tableName))
  ORDER BY c.databaseName, c.schemaName, c.tableName;

  -- Execute table-level DBCC commands
  BEGIN TRY
   RAISERROR(@sql, 0, 0) WITH NOWAIT;
         
   IF @debugMode = 0
   BEGIN
    UPDATE [dbo].[CheckTableStatus] SET startDate = GETDATE() WHERE checkTableID = @tblid;
    
    IF OBJECT_ID(@tbl) IS NOT NULL
     EXEC sp_ExecuteSQL @sql;

    UPDATE [dbo].[CheckTableStatus] SET procFlag = CASE ISNULL(OBJECT_ID(@tbl), 0) WHEN 0 THEN NULL ELSE 1 END, endDate = GETDATE() WHERE checkTableID = @tblid;
   END
   ELSE
    UPDATE [dbo].[CheckTableStatus] SET procFlag = NULL WHERE checkTableID = @tblid;
          
   IF @end < GETDATE()
   BEGIN
    SET @msg = 'Procedure has exceeded max run time based on @maxDuration parameter and will exit at ' + CONVERT(VARCHAR, GETDATE()) + '.';
     RAISERROR(@msg, 0, 0) WITH NOWAIT;
      RETURN;
   END
  END TRY
  
  BEGIN CATCH
   SET @msg = 'Failed to execute command {' + @sql + '} with error number: ' + CAST(ERROR_NUMBER() AS VARCHAR) + '; error message: ' + 
      ERROR_MESSAGE() + '.  Procedure terminated at ' + CONVERT(VARCHAR, GETDATE()) + '.';
    RAISERROR(@msg, 16, 2) WITH LOG, NOWAIT;
     RETURN(-2);  
  END CATCH
 END
END
--
--End While Loop 2
--

IF @debugMode = 1
 UPDATE dbo.CheckTableStatus SET procFlag = 0, startDate = NULL, endDate = NULL WHERE procFlag IS NULL;

SET @msg = CHAR(10) + CHAR(13) + 'DBCC job on ' + @@SERVERNAME + ' ended at ' + CONVERT(VARCHAR, GETDATE()) + '.';
 RAISERROR(@msg, 0, 0) WITH NOWAIT;

GO
