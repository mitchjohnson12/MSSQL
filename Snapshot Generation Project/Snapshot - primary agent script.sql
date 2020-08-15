--use clarity;
---------------------------
--Configurable Parameters--
---------------------------
--Initialize configurable parameters
declare @emailList varchar(250) = 'mtjohnso@epic.com; Jahirul.Islam@BSWHealth.org; Drew.Garfield@BSWHealth.org; Samantha.Parsons@BSWHealth.org; Minh.Tran@BSWHealth.org; Joni.Milliron@BSWHealth.org' --who to send alerts to if anything goes wrong
declare @SLATime time = '05:30 am' --Set the parameter to the time the Daily Epic-BI batch starts

--Initialize parameters for ODBC configurations
declare @dsnName varchar(50) = 'BOEClarity'--Populates $dsnName in the powershell script. Name of the ODBC connection (e.g. BOEClarity)
declare @hostNameAry varchar(50) = 'BSWEPICBOEP102,BSWEPICBOEP103'--populates $hostNameAry in the powershell script. Comma delimited list (NO SPACES) of hostnames that contain the ODBC connection (e.g. "BSWEPICBOEP102,BSWEPICBOEP103")
declare @primaryClarityServer varchar(50) = 'BSWEPICCLARP03' --populates $odbcConfig_Server in the powershell script. Used to update the ODBC connection to the primary server (e.g. BSWEPICCLARP03)
declare @secondaryClarityServer varchar(50) = 'BSWEPICCLARP04' --populates $odbcConfig_Server in the powershell script. Used to update the ODBC connection to the secondary server (e.g. BSWEPICCLARP04)
declare @odbcConfig_Platform varchar(10) = 'All'--$odbcConfig_Platform - 64-bit, 32-bit, or All 
declare @odbcUpdateFilePath varchar(100) = '"C:\Snapshot Management\UpdateODBC.ps1"' --Powershell script to update ODBC. If file path contains spaces, include double quotes around the path

--Initialize parameters for calling the snapshot agent on the secondary node
DECLARE @linkedServer nvarchar(50) = @secondaryClarityServer
DECLARE @jobName_createSnapshot nvarchar(100) = 'BSWH Clarity - SAN Snapshot Powershell Script' --The job on the secondary node that offlines the database, runs the powershell script to perform the san snapshot, then onlines the database 

-------------------------------
--End Configurable Parameters--
-------------------------------

--Initialized other parameters
declare @today date = cast(getdate() as date)
declare @message varchar(1000)
declare @errCnt int

--Initialize temp tables
DROP TABLE IF EXISTS #criticalTables, #TMP, #psOutput
create table #criticalTables (tableName varchar(50)) --List of critical tables according to BI_DEPENDENCIES
create table #TMP (tableName varchar(50), [status] varchar(20)) --ETL status of critical tables
create table #psOutput ([output] varchar(255)) --Captures errors of powershell script if any occur. Used to determine success of updateODBC.ps1 script.


--Quit if we've already created today's snapshot or if snapshot creation timed out
IF	EXISTS (SELECT * FROM master.dbo.snapshotCreationTimes WHERE creationDateTime > @today ) --Snapshot already created today
	OR EXISTS (SELECT * FROM master.dbo.snapshotAgentRunMsgs where runDate = @today and runMsg like 'ACTION REQUIRED: Snapshot creation timed out%') --snapshot timed out - manual intervention required
BEGIN
	RETURN
END

--Don't create snapshot if execution has not started today
IF NOT EXISTS (	select * from CR_STAT_EXECUTION 
				where (exec_descriptor like '%E0Q~100~%' or EXEC_DESCRIPTOR not like '%E0Q~%')
				and exec_start_time >= @today)
BEGIN
	SET @message = 'Clarity Nightly ETL execution has not started. Sign into the Clarity Console to troubleshoot. A P2 SNOW alert has been created through the Clarity ETL Health sql agent.'

	IF NOT EXISTS (select * from master.dbo.snapshotAgentRunMsgs where runDate = @today and runMsg = 'Clarity Nightly ETL execution has not started.')
	BEGIN	
		EXEC msdb.dbo.sp_send_dbmail @recipients = @emailList, @subject = 'ACTION REQUIRED: Clarity Snapshot Has Not Been Created', @body = @message
	END
	
	INSERT INTO master.dbo.snapshotAgentRunMsgs (runDate, runTime, runMsg)  VALUES (@today, cast(getdate() as time), @message)

	RETURN --choosing to not create a P2 incident ticket because it should already be created by the Clarity ETL Health SQL agent
END

--Alert for missed SLA
IF (cast(getdate() as time) > @SLATime) AND NOT EXISTS (SELECT * FROM master.dbo.snapshotAgentRunMsgs where rundate = @today and runMsg like 'Missed SLA%')
BEGIN
	--Send email alert about missed SLA
	SET @message = 'The Epic-BI batch has started, but the snapshot has not yet been created. Epic-BI reports will run against the primary Clarity server (' + @primaryClarityServer + ') '
					+ 'until the snapshot is created. Use master.dbo.snapshotAgentRunMessages to determine what is preventing the snapshot from being created.'
	INSERT INTO master.dbo.snapshotAgentRunMsgs (runDate, runTime, runMsg)  VALUES (@today, cast(getdate() as time), 'Missed SLA - ' + @message)
	EXEC msdb.dbo.sp_send_dbmail @recipients = @emailList, @subject = 'Late Snapshot Alert', @body = @message;
END

--Gather list of tables
INSERT INTO #criticalTables 
SELECT DISTINCT D.[OBJECT_NAME] 'TableName'
FROM V_BI_DEPEND_INFO V
INNER JOIN BI_DEPENDENCIES D ON 
	V.BI_DEPEND_ID = D.BI_DEPEND_ID AND 
	V.LATEST_CONTACT_DATE_REAL = D.CONTACT_DATE_REAL
WHERE V.DATA_MODEL_TYPE_C = 1 --CLARITY
	AND D.BI_OBJ_DEPEND_TYPE_C = 1 --TABLE/VIEW

--Collect statuses of tables used by downstream tables. Note that access logging tables are not included because they are run in E0Qs 101/108/109
--ETLs
insert into #TMP
select distinct ext.table_name, ext.[status]
from cr_stat_extract ext
	inner join #criticalTables ct on ct.tableName = ext.TABLE_NAME
where (exec_descriptor like '%E0Q~100~%' or EXEC_DESCRIPTOR not like '%E0Q~%') --Catches nightly ETL, fix ETL, and console-based executions
       and initialize_time > @today
	   and (initialize_time = (SELECT MAX(INITIALIZE_TIME) as expr1
            FROM CR_STAT_EXTRACT as i
            WHERE (ext.TABLE_NAME = TABLE_NAME)
			and initialize_time > @today
			and (exec_descriptor like '%E0Q~100~%' or EXEC_DESCRIPTOR not like '%E0Q~%')
			)) --handles the case where there are multiple ETLs for a single table by only grabbing the latest value
 
--Derived Tables
insert into #TMP
select distinct dtbl.table_name, dtbl.[status]
from CR_STAT_DERTBL dtbl
	inner join #criticalTables ct on ct.tableName = dtbl.TABLE_NAME
where (exec_descriptor like '%E0Q~100~%' or EXEC_DESCRIPTOR not like '%E0Q~%')
       and ckpt_init > @today
	   and (ckpt_init = (SELECT MAX(ckpt_init) as expr1
            FROM CR_STAT_DERTBL as i
            WHERE (dtbl.TABLE_NAME = TABLE_NAME)
			and ckpt_init > @today
			and (exec_descriptor like '%E0Q~100~%' or EXEC_DESCRIPTOR not like '%E0Q~%')
			)) --handles the case where there are multiple runs for a single table by only grabbing the latest value


--Alert for ETLErrors
IF EXISTS (SELECT * FROM #TMP WHERE [STATUS] NOT IN ('Success', 'Warning', 'InProgress'))
BEGIN
	--Character limit check for @message
	SET @message = 'ETL ERROR: The following tables errored or were aborted and are preventing snapshot creation: '
                     + (select STUFF((select top 15 ', ' + tableName from #TMP where [status] NOT IN ('Success', 'Warning', 'InProgress') for XML PATH('')), 1, 2, ''))
	set @errCnt = (select count(*) from #TMP WHERE [STATUS] NOT IN ('Success', 'Warning', 'InProgress'))
	if @errCnt > 15 
	BEGIN 
		SET @message = @message + '... and others' 
	END

	--Only send email and create P2 if alert has NOT already been sent today
	IF NOT EXISTS (SELECT * from master.dbo.snapshotAgentRunMsgs where runDate = @today and runMsg like 'ETL ERROR%')
	BEGIN
		INSERT INTO master.dbo.snapshotAgentRunMsgs (runDate, runTime, runMsg)
		VALUES (@today, cast(getdate() as time), @message)
		EXEC msdb.dbo.sp_send_dbmail @recipients = @emailList, @subject = 'ACTION REQUIRED: Clarity Snapshot Has Not Been Created', @body = @message;
		RAISERROR (N'ETL errors are preventing snapshot creation', 18, 1) WITH LOG --Adds entry to SQL Server logs. Can be used to create a P2 SNOW ticket
	END
	ELSE --Add entry to snapshotAgentRunMsgs that shows current progress
	BEGIN
		INSERT INTO master.dbo.snapshotAgentRunMsgs (runDate, runTime, runMsg)
		VALUES (@today, cast(getdate() as time), @message)
	END
END


--Alert for In Progress ETLs
--Quit if tables are in progress but we haven't ran past our SLA
IF	EXISTS (SELECT * FROM #TMP WHERE [STATUS] = 'InProgress')
BEGIN 
	--Character limit check for @message
	SET @message = 'The following ETLs are still In Progress and are preventing snapshot creation: '
                     + (select STUFF((select top 15 ', ' + tableName from #TMP where [status] = 'InProgress' for XML PATH('')), 1, 2, ''))
	set @errCnt = (select count(*) from #TMP WHERE [STATUS] = 'InProgress')
	if @errCnt > 15 
	BEGIN 
		SET @message = @message + '... and others' 
	END

	INSERT INTO master.dbo.snapshotAgentRunMsgs (runDate, runTime, runMsg)  VALUES (@today, cast(getdate() as time), @message)
	RETURN
END

--Create snapshot if everything was successful
IF	NOT EXISTS (SELECT * FROM #TMP WHERE [STATUS] NOT IN ('Success', 'Warning')) 
BEGIN 
	--create snapshot
	declare @snapshotSuccess int
	BEGIN TRY
		exec master.dbo.xp_startLinkedServerAgent @linkedServer = @linkedServer, @jobName = @jobName_createSnapshot
	END TRY
	BEGIN CATCH
		set @message = ERROR_MESSAGE();
		if @message like 'Agent job runtime exceeded the @maxTime_s setting%'
		begin
			set @message = 'ACTION REQUIRED: Snapshot creation timed out. ODBCs will not be updated automatically. View the history of the "' + @jobName_createSnapshot + '" job on ' + @linkedServer + 
							' and manually update the BOE ODBCs to point to the secondary server after the job completes. If the "' + @jobName_createSnapshot+ '" job failed, rerun the job again manually' +
							' before updating ODBCs.'
			EXEC msdb.dbo.sp_send_dbmail @recipients = @emailList, @subject = 'ACTION REQUIRED: Clarity Snapshot Timed Out', @body = @message
		end
		else
		begin
			set @message = '(Informational only) Snapshot creation in the failed "$(ESCAPE_SQUOTE(JOBNAME))" agent job failed. Agent will automatically retry on next job recurrence. If snapshot failures continue, troubleshoot by reviewing master.dbo.snapshotAgentRunMsgs ' +
							'as well as the job histories of "$(ESCAPE_SQUOTE(JOBNAME))" on ' + @@SERVERNAME + ' and "' + @jobName_createSnapshot + '" on ' + @linkedServer + '. The following error was returned from xp_startLinkedServerAgent: ' + @message
			EXEC msdb.dbo.sp_send_dbmail @recipients = @emailList, @subject = 'Clarity snapshot failed. Retry will occur automatically.', @body = @message
		end
		INSERT INTO master.dbo.snapshotAgentRunMsgs (runDate, runTime, runMsg)  VALUES (@today, cast(getdate() as time), @message);
		RAISERROR (N'Snapshot creation failed.', 18, 1) WITH LOG --Adds entry to SQL Server logs. Can be used to create a P2 SNOW ticket
		RETURN;
	END CATCH

	--Update snapshot creation history
	INSERT INTO master.dbo.snapshotCreationTimes VALUES (GETDATE())
	INSERT INTO master.dbo.snapshotAgentRunMsgs (runDate, runTime, runMsg)  VALUES (@today, cast(getdate() as time), 'Snapshot created')

	--Send email notification
	SET @message = 'Snapshot created at ' + format(GETDATE(), 'hh:mm tt')
	EXEC msdb.dbo.sp_send_dbmail @recipients = @emailList, @subject = 'Clarity Snapshot Created', @body = @message
	
	--Update ODBC
	declare @psString varchar(500)= 'powershell.exe -ExecutionPolicy Unrestricted -file ' + @odbcUpdateFilePath + ' -dsnName ' + @dsnName +
								' -hostNameAry ' + @hostNameAry + ' -odbcConfig_Server ' + @secondaryClarityServer + ' -odbcConfig_Platform ' + @odbcConfig_Platform
	insert into #psOutput
		exec xp_cmdshell @psString

	--Alert if ODBC update fails
	IF exists (select * from #psOutput where [output] is not null)
	BEGIN
		set @message = 'The powershell script in the ''$(ESCAPE_SQUOTE(JOBNAME))'' sql agent job failed to update the BOE ODBC connections after the snapshot was created on ' + @secondaryClarityServer + '. ' 
					+ 'Please update ' + @hostNameAry + ' ODBCs to point to ' + @secondaryClarityServer + ' manually. '
					+ 'To troubleshoot, review the history of the ''$(ESCAPE_SQUOTE(JOBNAME))'' sql agent job'
	
		INSERT INTO master.dbo.snapshotAgentRunMsgs (runDate, runTime, runMsg)  VALUES (@today, cast(getdate() as time), @message)

		declare @psErrorMessage varchar(max)
		SET @psErrorMessage = @message + ' and the powershell error message below.' + char(10) + char(10) + 'The powershell error message is as follows:' +
					STUFF( (SELECT char(10) + [output] FROM #psOutput FOR XML PATH('')), 1, 0, '') 
		
		EXEC msdb.dbo.sp_send_dbmail @recipients = 'mtjohnso@epic.com' , @subject = 'ACTION REQUIRED: BOE ODBC Connections Not Updated', @body = @psErrorMessage

		RAISERROR (N'ODBC updates failed in the ''$(ESCAPE_SQUOTE(JOBNAME))'' sql agent job.', 18, 1) WITH LOG --Adds entry to SQL Server logs. Can be used to create a P2 SNOW ticket
	END
	ELSE
	BEGIN
		INSERT INTO master.dbo.snapshotAgentRunMsgs (runDate, runTime, runMsg)  VALUES (@today, cast(getdate() as time), 'ODBC updated to secondary (' + @secondaryClarityServer + ')')
	END

	RETURN
END
