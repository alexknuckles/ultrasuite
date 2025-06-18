[Setup]
AppName=ultrasuite
AppVersion=0.1.0-pre
DefaultDirName={pf}\ultrasuite
OutputDir=installer
OutputBaseFilename=ultrasuite-setup
Compression=lzma
SolidCompression=yes

[Tasks]
Name: "desktopicon"; Description: "Create a desktop shortcut"; Flags: unchecked
Name: "desktopurl"; Description: "Create a desktop shortcut to the ultrasuite page"; Flags: unchecked

[Files]
Source: "dist\\app.exe"; DestDir: "{app}"; Flags: ignoreversion

[Icons]
Name: "{group}\ultrasuite"; Filename: "{app}\app.exe"
Name: "{group}\ultrasuite Web"; Filename: "http://localhost:5000"
Name: "{userdesktop}\ultrasuite"; Filename: "{app}\app.exe"; Tasks: desktopicon
Name: "{userdesktop}\ultrasuite Web"; Filename: "http://localhost:5000"; Tasks: desktopurl

[Run]
Filename: "{app}\app.exe"; Description: "Launch ultrasuite"; Flags: nowait postinstall skipifsilent
