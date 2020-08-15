--use master;
CREATE PROCEDURE dbo.xp_startLinkedServerAgent
	@linkedServer nvarchar(50), 
	@jobName nvarchar(100)
/*
Developed By: Mitch Johnson - Sept 2019
Description: This stored procedure will call a SQL agent on a linked server and wait until it 
	completes before returning a success flag. It was created to support Clarity snapshot creation.
Parameters:
	@linkedServer - Linked server to execute the agent job. 
	@jobName - Name of the agent job to execute. The agent job must exist on the linked server.
Output
	1 - success
	0 - fail
*/
AS
IF ((@linkedServer is null) or (@jobName is null))
BEGIN
	;THROW 100000, '@linkedServer and @jobName must be specified', 1
	RETURN 0
END

DECLARE @sql nvarchar(1000)
DECLARE @maxTime_s int = 1800 --maximum amount of time to wait for the SQL agent to complete. If time is exceeded, the stored procedure will return 0.
DECLARE @waitDuration_s int = 10 --amount of time to wait before re-checking status of job run.

--Get the job_id
DECLARE @Job_Id varchar(100)
SET @sql = 'SELECT @result = job_id 
			FROM [' + @linkedServer + '].[msdb].[dbo].[sysjobs] 
			WHERE [name] = ''' + @jobName + ''''
EXEC sp_executesql @sql, N'@result varchar(100) OUT', @Job_Id out 
IF (@Job_Id is null)
BEGIN
	;THROW 100000, 'Specified @Job_Id was not found on the linked server', 1
	RETURN 0
END

--Get the greatest outcome instance_id
DECLARE @Instance_Id varchar(100)
SET @sql = 'SELECT @result = MAX(instance_id) 
			FROM [' + @linkedServer + '].[msdb].[dbo].[sysjobhistory] jh
			WHERE 
				jh.job_id = ''' + @Job_Id + '''
				AND jh.step_name = ''(Job outcome)'''
EXEC sp_executesql @sql, N'@result varchar(100) OUT', @Instance_Id out 	
IF (@Instance_Id is null) set @Instance_Id = 0 --handles the case where job has never been performed

--start the job
DECLARE @failedFlag INT ;
set @sql = 'EXECUTE @result= [' + @linkedServer +'].[msdb].[dbo].sp_start_job ''' + @jobName + '''' --sp_start_job counterintuitively returns 1 if it fails. See msn documentation.
EXEC sp_executesql @sql, N'@result int OUT', @failedFlag out
IF (@failedFlag = 1) or (@failedFlag is null)
BEGIN
	;THROW 100000, 'sp_start_job failed to start the agent job', 1
	RETURN 0
END

--loop until the job completes
DECLARE @loopsCounter int = 0
DECLARE @maxLoops int = @maxTime_s / @waitDuration_s 
declare @wait datetime = dateadd(SECOND, @waitDuration_s , convert(DATETIME, 0))
declare @run_status int;

set @sql = 'SELECT @result = jh.run_status
			FROM [' + @linkedServer + '].[msdb].[dbo].[sysjobhistory] jh
			WHERE 
				jh.job_id = ''' + @Job_Id + '''
				AND jh.step_name = ''(Job outcome)''
				AND jh.instance_id > ''' + @Instance_Id + ''' 
				AND jh.run_status != 2 --retry
				AND jh.run_status != 4 --in progress'

WAITFOR DELAY @wait
EXEC sp_executesql @sql, N'@result int OUT', @run_status out 

WHILE (@run_status is null)
BEGIN 
	set @loopsCounter = @loopsCounter + 1
	if @loopsCounter = @maxLoops break;
	WAITFOR DELAY @wait;
	EXEC sp_executesql @sql, N'@result int OUT', @run_status out 
END;

IF (@run_status is null)
BEGIN
	;THROW 100000, 'Agent job runtime exceeded the @maxTime_s setting specified in the master.dbo.xp_startLinkedServerAgent stored procedure.', 1
	RETURN 0	
END
ELSE IF (@run_status in (0,3))
BEGIN
	;THROW 100000, 'The linked job failed. Please check the "' + @jobName + '" agent job''s history on the '+ @linkedServer +' server to determine the root cause.', 1
	RETURN 0
END
ELSE
	RETURN 1
GO;