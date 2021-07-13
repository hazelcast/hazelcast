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
set HAZELCAST_HOME=%~dp0..

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

FOR /F "tokens=* USEBACKQ" %%F IN (`CALL "%RUN_JAVA%" -cp "%HAZELCAST_HOME%\lib\hazelcast-${project.version}.jar" com.hazelcast.internal.util.JavaVersion`) DO SET JAVA_VERSION=%%F

IF NOT "%JAVA_VERSION%" == "8" (
	set JAVA_OPTS=%JAVA_OPTS% --add-modules java.se --add-exports java.base/jdk.internal.ref=ALL-UNNAMED --add-opens java.base/java.lang=ALL-UNNAMED --add-opens java.base/java.nio=ALL-UNNAMED --add-opens java.base/sun.nio.ch=ALL-UNNAMED --add-opens java.management/sun.management=ALL-UNNAMED --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED
)

set JAVA_OPTS=%JAVA_OPTS%^
 "-Dhazelcast.logging.type=log4j2"^
 "-Dlog4j.configurationFile=file:%HAZELCAST_HOME%\config\log4j2.properties"^
 "-Dhazelcast.config=%HAZELCAST_HOME%\config\hazelcast.xml"^
 "-Dhz.home=%HAZELCAST_HOME%"

set CLASSPATH="%HAZELCAST_HOME%\lib\*;%HAZELCAST_HOME%\bin\user-lib;%HAZELCAST_HOME%\bin\user-lib\*";%CLASSPATH%

ECHO ########################################
ECHO # RUN_JAVA=%RUN_JAVA%
ECHO # JAVA_OPTS=%JAVA_OPTS%
ECHO # CLASSPATH=%CLASSPATH%
ECHO ########################################
ECHO Starting Hazelcast

"%RUN_JAVA%" %JAVA_OPTS% -cp "%CLASSPATH%" "com.hazelcast.core.server.HazelcastMemberStarter"

ENDLOCAL
