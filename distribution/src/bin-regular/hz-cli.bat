@echo off

SETLOCAL ENABLEDELAYEDEXPANSION

CALL "%~dp0common.bat"

if %errorlevel% neq 0 (
    exit /b %errorlevel%
)

echo "%RUN_JAVA%" %displayOpts% -cp %CLASSPATH% com.hazelcast.client.console.HazelcastCommandLine %*
"%RUN_JAVA%" %filteredOpts% -cp %CLASSPATH% com.hazelcast.client.console.HazelcastCommandLine %*

ENDLOCAL
