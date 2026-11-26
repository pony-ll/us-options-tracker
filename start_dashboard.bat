@echo off
chcp 65001 >nul
cd /d "%~dp0"
title US Options Tracker
echo Starting local dashboard service, browser will open automatically...
python serve.py
if errorlevel 1 (
  echo.
  echo Failed to start. Check that Python and requests are installed:
  echo   pip install requests
)
pause
