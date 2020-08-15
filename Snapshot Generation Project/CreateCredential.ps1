$pw = convertto-securestring -AsPlainText -Force -String "**Insert Password Here**"
$cred = new-object -typename System.Management.Automation.PSCredential -argumentlist "EpicBOE",$pw
$cred | Export-CliXml -Path "C:\Snapshot Management\EpicBOE_userCredential.xml" 