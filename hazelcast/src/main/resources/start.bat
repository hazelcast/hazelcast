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

set "CLASSPATH=%~dp0..\lib\hazelcast-all-${project.version}.jar"

FOR /F "tokens=* USEBACKQ" %%F IN (`tasklist /FI "WINDOWTITLE eq hazelcast %CLASSPATH%" 2^>NUL ^| find /I /C "java"`) DO (
SET COUNT=%%F
)
IF NOT "%COUNT%"=="0" (
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
ECHO Another Hazelcast instance is already started in this folder. To start a new instance, please unzip the zip file in a new folder.
pause

:endofscript

ENDLOCAL
