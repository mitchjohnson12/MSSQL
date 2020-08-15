use master;
create table master.dbo.snapshotCreationTimes (creationDateTime datetime)
create table master.dbo.snapshotAgentRunMsgs (runDate date, runTime time, runMsg varchar(1000))