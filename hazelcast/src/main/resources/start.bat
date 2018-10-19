@echo off

SETLOCAL

if NOT DEFINED JAVA_HOME goto error
set RUN_JAVA=%JAVA_HOME%\bin\java


REM ******* you can enable following variables by uncommenting them

REM ******* minimum heap size
REM set MIN_HEAP_SIZE=1G

REM ******* maximum heap size
REM set MAX_HEAP_SIZE=1G


if NOT "%MIN_HEAP_SIZE%" == "" (
	set JAVA_OPTS=%JAVA_OPTS% -Xms%MIN_HEAP_SIZE%
)

if NOT "%MAX_HEAP_SIZE%" == "" (
	set JAVA_OPTS=%JAVA_OPTS% -Xmx%MAX_HEAP_SIZE%
)

set "CLASSPATH=%~dp0..\lib\hazelcast-all-${project.version}.jar;%~dp0..\user-lib;%~dp0..\user-lib\*"

FOR /F "tokens=2 delims=," %%F in ('tasklist /NH /FI "WINDOWTITLE eq hazelcast %CLASSPATH%" /fo csv') DO (
SET PID=%%F
)
IF NOT "%PID%"=="" (
    goto alreadyrunning
)

ECHO ########################################
ECHO # RUN_JAVA=%RUN_JAVA%
ECHO # JAVA_OPTS=%JAVA_OPTS%
ECHO # starting now...."
ECHO ########################################

start "hazelcast %CLASSPATH%" "%RUN_JAVA%" %JAVA_OPTS% -cp "%CLASSPATH%" "com.hazelcast.core.server.StartServer"
goto endofscript

:error
ECHO JAVA_HOME environment variable must be set!
pause

:alreadyrunning
ECHO Another Hazelcast instance (PID=%PID%) is already started in this folder. To start a new instance, please unzip the zip file in a new folder.
pause

:endofscript

ENDLOCAL