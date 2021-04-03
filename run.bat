@setlocal
@if not "%1"=="-verbose" goto SILENT

set VERBOSE=-verbose
SHIFT
goto VERBOSE

:SILENT

@echo off

:VERBOSE

rem *** store environment

:START

set MAINFILE=main.py
cd bin
python %MAINFILE%
goto end

:END
PAUSE

rem *** restore environment
endlocal
