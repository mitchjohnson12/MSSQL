param (
    [string]$dsnName = $(throw "-dsnName is required."), #Name of the ODBC connection (e.g. BOEClarity)
    [string]$hostNameAry = $(throw "-hostNameAry is required."), #Comma-delimited array (no spaces) of hostnames that contains the ODBC connection (e.g. BSWEPICBOEP102,BSWEPICBOEP103)
    [string]$odbcConfig_Server = $(throw "-odbcConfig_Server is required."), #The server that the ODBC connection should be updated to point to (e.g. BSWEPICCLARP04)
    [string]$odbcConfig_Platform = $(throw "-odbcConfig_Platform is required.") #64-bit, 32-bit, or All
)
$odbcConfig_Server = -join("Server=", $odbcConfig_Server)

#gather credentials
$cred = Import-CliXml -Path "C:\Snapshot Management\EpicBOE_userCredential.xml"

#update odbc on $hostName
foreach ($hostName in $hostNameAry.split(',')) 
{   
    $s = new-pssession -computername $hostName -credential $cred
    Invoke-Command -Session $s -Scriptblock {Set-OdbcDsn -Name $args[0] -DsnType "System" -Platform $args[1] -SetPropertyValue $args[2]} -argumentlist $dsnName,$odbcConfig_Platform,$odbcConfig_Server
    Remove-PSSession $s
}