---------------------------
--Configurable Parameters--
---------------------------
--Initialize configurable parameters
declare @emailList_Clarity varchar(250) = 'mtjohnso@epic.com'
declare @emailList_BOE varchar(250) = 'mtjohnso@epic.com'
declare @SLATime time = '11:30 pm' --if ETL has not completed by this time, we will update BOE ODBCs to all point to primary node and 

--Initialize parameters for ODBC configurations
declare @dsnName varchar(50) = 'BOEClarity'--Populates $dsnName in the powershell script. Name of the ODBC connection (e.g. BOEClarity)
declare @hostNameAry varchar(50) = 'BSWEPICBOEP202,BSWEPICBOEP203'--populates $hostNameAry in the powershell script. Comma delimited list (NO SPACES) of hostnames that contain the ODBC connection (e.g. "BSWEPICBOEP102,BSWEPICBOEP103")
declare @primaryClarityServer varchar(50) = 'BSWEPICCLARP203' --populates $odbcConfig_Server in the powershell script. Used to update the ODBC connection to the primary server (e.g. BSWEPICCLARP03)
declare @secondaryClarityServer varchar(50) = 'BSWEPICCLARP202' --populates $odbcConfig_Server in the powershell script. Used to update the ODBC connection to the secondary server (e.g. BSWEPICCLARP04)
declare @odbcConfig_Platform varchar(10) = 'All'--$odbcConfig_Platform - 64-bit, 32-bit, or All 
declare @odbcUpdateFilePath varchar(100) = '"C:\Snapshot Management\UpdateODBC.ps1"' --Powershell script to update ODBC. If file path contains spaces, include double quotes around the path

--Initialize parameters for calling the snapshot agent on the secondary node
DECLARE @linkedServer nvarchar(50) = @secondaryClarityServer
DECLARE @jobName_createSnapshot nvarchar(100) = '_snaptest'

-------------------------------
--End Configurable Parameters--
-------------------------------
