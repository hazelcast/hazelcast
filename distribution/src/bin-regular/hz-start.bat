@echo off

SETLOCAL ENABLEDELAYEDEXPANSION

CALL "%~dp0common.bat"
if %errorlevel% neq 0 (
    exit /b %errorlevel%
)

ECHO ########################################
ECHO # RUN_JAVA=%RUN_JAVA%
ECHO # JAVA_OPTS=!displayOpts!
ECHO # CLASSPATH=%CLASSPATH%
ECHO ########################################
ECHO Starting Hazelcast

"%RUN_JAVA%" !filteredOpts! -cp %CLASSPATH% "com.hazelcast.core.server.HazelcastMemberStarter"

ENDLOCAL
