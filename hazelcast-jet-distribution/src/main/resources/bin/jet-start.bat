@echo off

SETLOCAL

if NOT DEFINED JAVA_HOME goto error
set RUN_JAVA=%JAVA_HOME%\bin\java
set JET_HOME=%~dp0..

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

set JAVA_OPTS=%JAVA_OPTS%^
 "-Dhazelcast.jet.config=%JET_HOME%\config\hazelcast-jet.xml"^
 "-Dhazelcast.config=%JET_HOME%\config\hazelcast.xml"^
 "-Djet.home=%JET_HOME%"

set CLASSPATH="%JET_HOME%\lib\${hazelcast.jet.artifact}-${project.version}.jar";%CLASSPATH%

ECHO ########################################
ECHO # RUN_JAVA=%RUN_JAVA%
ECHO # JAVA_OPTS=%JAVA_OPTS%
ECHO # CLASSPATH=%CLASSPATH%
ECHO # starting now....
ECHO ########################################

start "hazelcast-jet" "%RUN_JAVA%" %JAVA_OPTS% -cp %CLASSPATH% com.hazelcast.jet.server.StartServer"
goto endofscript

:error
ECHO JAVA_HOME environment variable must be set!
pause

:endofscript

ENDLOCAL