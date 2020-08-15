--Initialize parameters for ODBC configurations
declare @dsnName varchar(50) = 'BOEClarity'--$dsnName - Name of the ODBC connection (e.g. BOEClarity)
declare @hostNameAry varchar(50) = 'BSWEPICBOEP102,BSWEPICBOEP103'--populates $hostNameAry in the powershell script. Comma delimited list (NO COMMAS) of hostnames that contain the ODBC connection (e.g. "BSWEPICBOEP102,BSWEPICBOEP103")
declare @primaryClarityServer varchar(50) = 'BSWEPICCLARP03' --populates $odbcConfig_Server in the powershell script. Used to update the ODBC connection to the primary server (e.g. BSWEPICCLARP03)
declare @secondaryClarityServer varchar(50) = 'BSWEPICCLARP04' --populates $odbcConfig_Server in the powershell script. Used to update the ODBC connection to the secondary server (e.g. BSWEPICCLARP04)
declare @odbcConfig_Platform varchar(10) = 'All'--$odbcConfig_Platform - 64-bit, 32-bit, or All 
declare @odbcUpdateFilePath varchar(100) = '"C:\Snapshot Management\UpdateODBC.ps1"' --TODO: hide the credentials in the powershell script

declare @emailList varchar(200) = 'mtjohnso@epic.com; Jahirul.Islam@BSWHealth.org; Drew.Garfield@BSWHealth.org; Samantha.Parsons@BSWHealth.org; Minh.Tran@BSWHealth.org; Joni.Milliron@BSWHealth.org' --who receives an alert if something goes wrong

drop table if exists #psOutput
create table #psOutput ([output] varchar(255)) --Captures errors of powershell script if any occur. Used to determine success of updateODBC.ps1 script.

declare @psString varchar(500)= 'powershell.exe -ExecutionPolicy Unrestricted -file ' + @odbcUpdateFilePath + ' -dsnName ' + @dsnName +
							' -hostNameAry ' + @hostNameAry + ' -odbcConfig_Server ' + @primaryClarityServer + ' -odbcConfig_Platform ' + @odbcConfig_Platform
insert into #psOutput
	exec xp_cmdshell @psString

IF exists (select * from #psOutput where [output] is not null)
BEGIN
	DECLARE @message varchar(1000) = 'The powershell script failed to update the BOE ODBC connections before taking the Clarity database on ' + @secondaryClarityServer + ' OFFLINE. ' 
									+ 'Please update ' + @hostNameAry + ' ODBCs to point to ' + @primaryClarityServer + ' manually. '
									+ 'To troubleshoot, review the history of the ''$(ESCAPE_SQUOTE(JOBNAME))'' sql agent job.'
	
	INSERT INTO master.dbo.snapshotAgentRunMsgs (runDate, runTime, runMsg)  VALUES (cast(getdate() as date), cast(getdate() as time), @message)

	declare @psErrorMessage varchar(max)
	SET @psErrorMessage = @message + char(10) + char(10) + 'The powershell error message is as follows:' + 
				STUFF( (SELECT char(10) + [output] FROM #psOutput FOR XML PATH('')), 1, 1, '') 
		
	EXEC msdb.dbo.sp_send_dbmail @recipients = @emailList , @subject = 'BOE ODBC Connections Not Updated', @body = @psErrorMessage

	RAISERROR (N'ODBC updates failed in the ''$(ESCAPE_SQUOTE(JOBNAME))'' sql agent job.', 18, 1) WITH LOG --creates P2 SNOW ticket
END

