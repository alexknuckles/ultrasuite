[Setup]
AppName=ultrasuite
AppVersion=1.0
DefaultDirName={pf}\ultrasuite
OutputDir=installer
OutputBaseFilename=ultrasuite-setup
Compression=lzma
SolidCompression=yes

[Files]
Source: "dist\\app.exe"; DestDir: "{app}"; Flags: ignoreversion

[Icons]
Name: "{group}\ultrasuite"; Filename: "{app}\app.exe"

[Run]
Filename: "{app}\app.exe"; Description: "Launch ultrasuite"; Flags: nowait postinstall skipifsilent
