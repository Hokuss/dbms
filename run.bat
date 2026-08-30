@echo off
:: 1. Configure
cmake -B build
if %ERRORLEVEL% NEQ 0 exit /b %ERRORLEVEL%

:: 2. Build
cmake --build build
if %ERRORLEVEL% NEQ 0 exit /b %ERRORLEVEL%

:: 3. Run
echo.
echo === Running Executable ===
.\build\sandbox.exe