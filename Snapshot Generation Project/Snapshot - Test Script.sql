/*
----------------
--Instructions--
----------------
To use these test scripts, 
1. Save a local copy of this .SQL file
2. Copy and paste the ETL Alert agent code to the bottom of the editor
3. Use find and replace to replace all mentions of CR_STAT_EXTRACT with #CR_STAT_EXTRACT
4. Use find and replace to replace all mentions of CR_STAT_EXECUTION with #CR_STAT_EXECUTION
5. Use find and replace to replace all mentions of CR_STAT_DERTBL with #CR_STAT_DERTBL
6. Update emailList to only send to yourself
7. Copy and paste a scenario to the top of the agent query. 
8. If necessary, update the agent run parameters per the test scenario instructions.
9. Run the test scenario and verify the expected outcome is acheived.
10. If necessary, back out the parameter updates from step 8 before proceeding to the next step.
11. Rerun steps 7-10 until all test scenarios are completed.


------------------
--TEST SCENARIOS--
------------------
--Scenario 1 - All successful  
declare @etlStartTime datetime = dateadd(hour, 1, cast(cast(getdate() as date) as datetime))
drop table if exists #CR_STAT_EXECUTION
create table #CR_STAT_EXECUTION (exec_descriptor varchar(50), exec_start_time datetime)
INSERT INTO #CR_STAT_EXECUTION VALUES ('sw_rptshd E0Q~100~mtjtest', @etlStartTime)

drop table if exists #CR_STAT_EXTRACT
create table #CR_STAT_EXTRACT (table_name varchar(50), exec_descriptor varchar(50), initialize_time datetime, [status] varchar(20))
insert into #CR_STAT_EXTRACT values
('PAT_ENC', 'sw_rptshd E0Q~100~mtjtest', @etlStartTime, 'Warning'), 
('PATIENT', 'sw_rptshd E0Q~100~mtjtest', @etlStartTime, 'Warning'),
('PATIENT_4', 'sw_rptshd E0Q~100~mtjtest', @etlStartTime, 'Success'),
('PAT_ACTIVE_REG', 'sw_rptshd E0Q~100~mtjtest', @etlStartTime, 'Success'), 
('PAT_ENC_DX', 'sw_rptshd E0Q~100~mtjtest', @etlStartTime, 'Warning'),
('HSP_ACCOUNT', 'sw_rptshd E0Q~100~mtjtest', @etlStartTime, 'Success'),
('HSP_TRANSACTIONS', 'sw_rptshd E0Q~100~mtjtest', @etlStartTime, 'Success'),
('PATIENT_ADDR_AUDIT', 'sw_rptshd E0Q~100~mtjtest', @etlStartTime, 'InProgress') --Not in BI_DEPENDENCIES

drop table if exists #CR_STAT_DERTBL
create table #CR_STAT_DERTBL (table_name varchar(50), exec_descriptor varchar(50), ckpt_init datetime, [status] varchar(20))
insert into #CR_STAT_DERTBL values
('F_ARHB_INACTIVE_TX', 'sw_rptshd E0Q~100~mtjtest', @etlStartTime, 'Success'), 
('F_HM_TREND', 'sw_rptshd E0Q~100~mtjtest', @etlStartTime, 'Warning'),	
('F_SCHED_APPT', 'sw_rptshd E0Q~100~mtjtest', @etlStartTime, 'Warning'),
('F_ADT_TX', 'sw_rptshd E0Q~100~mtjtest', @etlStartTime, 'Error') --Not in BI_DEPENDENCIES

/*
Expected outcomes: 
	Entry added to master.dbo.snapshotCreationTimes 
	Entries added to master.dbo.snapshotAgentRunMsgs
	 - snapshot creation time
	 - time that odbc switched
	Email sent to @emailList that says that the snapshot has been created
Result: PASSED
*/

--Scenario 2 - Snapshot already created
--Instruction: Rerun the above test
--Expected outcome: nothing happens (quits because snapshot has already been created)
--Result: PASSED
--Post-Scenario Instruction: truncate table master.dbo.snapshotCreationTimes



--Scenario 3 - Execution did not run today
drop table if exists #CR_STAT_EXECUTION
create table #CR_STAT_EXECUTION (exec_descriptor varchar(50), exec_start_time datetime)
INSERT INTO #CR_STAT_EXECUTION VALUES ('sw_rptshd E0Q~100~mtjtest', dateadd(day, -1, getdate()))
/*
Expected outcome: add entry to master.dbo.snapshotAgentRunMsgs, send email
Result: PASSED

Rerun: add entry to master.dbo.snapshotAgentRunMsgs, no email.
Result: PASSED
*/


--Scenario 4 - Errored ETLs
declare @etlStartTime datetime = dateadd(hour, 1, cast(cast(getdate() as date) as datetime))
truncate table master.dbo.snapshotAgentRunMsgs
truncate table master.dbo.snapshotCreationTimes

drop table if exists #CR_STAT_EXECUTION
create table #CR_STAT_EXECUTION (exec_descriptor varchar(50), exec_start_time datetime)
INSERT INTO #CR_STAT_EXECUTION VALUES ('sw_rptshd E0Q~100~mtjtest', getdate())

drop table if exists #CR_STAT_EXTRACT
create table #CR_STAT_EXTRACT (table_name varchar(50), exec_descriptor varchar(50), initialize_time datetime, [status] varchar(20))
insert into #CR_STAT_EXTRACT values
('PAT_ENC', 'sw_rptshd E0Q~100~mtjtest', @etlStartTime, 'Error'), 
('PATIENT', 'sw_rptshd E0Q~100~mtjtest', @etlStartTime, 'Warning'),
('PATIENT_4', 'sw_rptshd E0Q~100~mtjtest', @etlStartTime, 'Success'),
('PAT_ACTIVE_REG', 'sw_rptshd E0Q~100~mtjtest', @etlStartTime, 'Success'), 
('PAT_ENC_DX', 'sw_rptshd E0Q~100~mtjtest', @etlStartTime, 'Warning'),
('HSP_ACCOUNT', 'sw_rptshd E0Q~100~mtjtest', @etlStartTime, 'Success'),
('HSP_TRANSACTIONS', 'sw_rptshd E0Q~100~mtjtest', @etlStartTime, 'Success'),
('PATIENT_ADDR_AUDIT', 'sw_rptshd E0Q~100~mtjtest', @etlStartTime, 'InProgress') --Not in BI_DEPENDENCIES

drop table if exists #CR_STAT_DERTBL
create table #CR_STAT_DERTBL (table_name varchar(50), exec_descriptor varchar(50), ckpt_init datetime, [status] varchar(20))
insert into #CR_STAT_DERTBL values
('F_ARHB_INACTIVE_TX', 'sw_rptshd E0Q~100~mtjtest', @etlStartTime, 'Error'), --errored ETL 
('F_HM_TREND', 'sw_rptshd E0Q~100~mtjtest', @etlStartTime, 'Warning'),	
('F_SCHED_APPT', 'sw_rptshd E0Q~100~mtjtest', @etlStartTime, 'Warning'),
('F_ADT_TX', 'sw_rptshd E0Q~100~mtjtest', @etlStartTime, 'Error') --Not in BI_DEPENDENCIES


/*
Expected Outcome: 
	email sent that shows errored tables
	error raised (P2 incident created) 
	row added to master.dbo.snapshotAgentRunMsgs that shows errored tables
	Result: PASSED
*/


--Scenario 5 - rerun scenario 4
--Required Setup: Remove the two truncate table statements before rerunning
--Expected outcome: row added to master.dbo.snapshotAgentRunMsgs, no email sent, no error raised
--Result: PASSED


--Scenario 6 - Tables in progress, before SLA
--Instruction: update @SLATime to 11:30pm
declare @etlStartTime datetime = dateadd(hour, 1, cast(cast(getdate() as date) as datetime))
drop table if exists #CR_STAT_EXECUTION
create table #CR_STAT_EXECUTION (exec_descriptor varchar(50), exec_start_time datetime)
INSERT INTO #CR_STAT_EXECUTION VALUES ('sw_rptshd E0Q~100~mtjtest', @etlStartTime)

drop table if exists #CR_STAT_EXTRACT
create table #CR_STAT_EXTRACT (table_name varchar(50), exec_descriptor varchar(50), initialize_time datetime, [status] varchar(20))
insert into #CR_STAT_EXTRACT values
('PAT_ENC', 'sw_rptshd E0Q~100~mtjtest', @etlStartTime, 'InProgress'), --InProgress
('PATIENT', 'sw_rptshd E0Q~100~mtjtest', @etlStartTime, 'Warning'),
('PATIENT_4', 'sw_rptshd E0Q~100~mtjtest', @etlStartTime, 'Success'),
('PAT_ACTIVE_REG', 'sw_rptshd E0Q~100~mtjtest', @etlStartTime, 'InProgress'), --InProgress
('PAT_ENC_DX', 'sw_rptshd E0Q~100~mtjtest', @etlStartTime, 'Warning'),
('HSP_ACCOUNT', 'sw_rptshd E0Q~100~mtjtest', @etlStartTime, 'Success'),
('HSP_TRANSACTIONS', 'sw_rptshd E0Q~100~mtjtest', @etlStartTime, 'Success'),
('PATIENT_ADDR_AUDIT', 'sw_rptshd E0Q~100~mtjtest', @etlStartTime, 'InProgress') --Not in BI_DEPENDENCIES

drop table if exists #CR_STAT_DERTBL
create table #CR_STAT_DERTBL (table_name varchar(50), exec_descriptor varchar(50), ckpt_init datetime, [status] varchar(20))
insert into #CR_STAT_DERTBL values
('F_ARHB_INACTIVE_TX', 'sw_rptshd E0Q~100~mtjtest', @etlStartTime, 'InProgress'), --InProgress
('F_HM_TREND', 'sw_rptshd E0Q~100~mtjtest', @etlStartTime, 'Warning'),	
('F_SCHED_APPT', 'sw_rptshd E0Q~100~mtjtest', @etlStartTime, 'Warning'),
('F_ADT_TX', 'sw_rptshd E0Q~100~mtjtest', @etlStartTime, 'InProgress') --Not in BI_DEPENDENCIES

/*
Expected outcomes: 
	Entry added to master.dbo.snapshotAgentRunMsgs.
	No email or alerts
Result: PASSED
*/
--Post-Scenario Instruction: restore @SLATime back to 5:30am.



--Scenario 7 - ETLs running after SLA
--Rerun scenario above, but without updating the @SLATime parameter.
/*
Expected outcome: 
	Email received for missed SLA 
	Row added to master.dbo.snapshotAgentRunMsgs for missed SLA
	Row added to master.dbo.snapshotAgentRunMsgs for long-running ETLs
Result: PASSED
*/

--Scenario 8 - Rerun scenario 7
/*
Expected outcome: 
	no error, no email
	row added to master.dbo.snapshotAgentRunMsgs
Result: PASSED
*/

--Scenario 9 - All successful after errors
--Instruction: rerun scenario 1
/*
Expected outcome: same as scenario 1
Result: PASSED
*/


--Scenario 10 - All ETLs complete. Update ODBC fails.
--Instruction: Update @hostNameAry = 'BSWEPICBOEP202,BSWEPICBOEP203,THISISNOTREAL' (or whatever the appropriate linked server is for your testing environment). This will cause the powershell script to fail.
--Instruction: Run scenario 1
--Post Instruction: revert @dsnName back to 'BOEClarity'
/*
Expected outcomes:
	Emails sent
		Snapshot created
		OCBC failed
	Entries added to master.dbo.snapshotAgentRunMsgs
		Snapshot created
		Update ODBC failed
	Entry added to master.dbo.snapshotCreationTimes
	Error raised
Result: PASSED
/*


--Scenario 11 - All ETLs complete. Snapshot job fails.
--Instruction: update @jobName to point to a job that is designed to fail. For example, the job step can call "RAISERROR (N'This is a test', 18, 1)".
--Instruction: Run scenario 1
/*
Expected outcomes:
	Email sent that says snapshot failed but it will retry
	Entry added to master.dbo.snapshotAgentRunMsgs that says the same
	Error raised
	No entry added to master.dbo.snapshotCreationTimes
Result: PASSED
*/

--Scenario 12 - All ETLs complete. Snapshot times out.
--Instruction: update @jobName to a job that waits 20 seconds then succeeds. E.g. WAITFOR DELAY '00:00:20'; RETURN; 
--Instruction: Modify master.dbo.xp_startLinkedServerAgent and set @maxTime_s = 10 and @waitDuration_s = 1 
/*
Expected outcomes
	Email sent that snapshot timed out and will need to be updated manually
	Entry added to master.dbo.snapshotAgentRunMsgs that says the same
	Error raised
	No entry added to master.dbo.snapshotCreationTimes
Result: PASSED
*/

--Scenario 13 - Rerun after snapshot timed out
/*
Expected outcome
	Nothing happens. Job quits at the beginning because we do not want to accidentally create a second snapshot.
Result: PASSED
*/

--ADD YOUR TEST SCENARIO BELOW:




--PASTE YOUR SQL AGENT CODE BELOW:






