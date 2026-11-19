@echo off
cd /d "%~dp0"
rem Start panel server (minimized window) if port 8778 is not busy
netstat -ano | findstr /c:"127.0.0.1:8778" | findstr LISTENING >nul
if errorlevel 1 (
    start "StreamChat server" /min python -u server.py
    timeout /t 3 /nobreak >nul
)
rem Separate window without tabs: drag it to the second monitor, F11 = fullscreen
start "" msedge --app=http://localhost:8778 --new-window
