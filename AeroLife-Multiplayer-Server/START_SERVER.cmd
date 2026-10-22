@echo off
cd /d "%~dp0"
where node >nul 2>nul
if errorlevel 1 (
  echo Install Node.js 22 LTS from https://nodejs.org/en/download
  pause
  exit /b 1
)
if not exist node_modules call npm ci --omit=dev --ignore-scripts
if errorlevel 1 goto end
node server.mjs
:end
pause
