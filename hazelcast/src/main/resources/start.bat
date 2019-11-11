@echo off

SETLOCAL

if "x%JAVA_HOME%" == "x" (
    echo JAVA_HOME environment variable not available.
    set RUN_JAVA=java
) else (
    set "RUN_JAVA=%JAVA_HOME%\bin\java"
)

"%RUN_JAVA%" -version 1>nul 2>nul || (
    echo JAVA could not be found in your system.
    echo Please install Java 1.8 or higher!!!
    exit /b 2
)

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

ECHO ########################################
ECHO # RUN_JAVA=%RUN_JAVA%
ECHO # JAVA_OPTS=%JAVA_OPTS%
ECHO # starting now...."
ECHO ########################################

start "hazelcast-imdg" "%RUN_JAVA%" %JAVA_OPTS% -cp "%CLASSPATH%" "com.hazelcast.core.server.HazelcastMemberStarter"

ENDLOCAL