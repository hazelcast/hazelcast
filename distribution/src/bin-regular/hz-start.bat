@echo off

SETLOCAL

if NOT DEFINED JAVA_HOME goto error
set RUN_JAVA=%JAVA_HOME%\bin\java
set HAZELCAST_HOME=%~dp0..

set JAVA_OPTS=%JAVA_OPTS%^
 "-Dhazelcast.logging.type=log4j2"^
 "-Dlog4j.configurationFile=file:%HAZELCAST_HOME%\config\log4j2.properties"^
 "-Dhazelcast.config=%HAZELCAST_HOME%\config\hazelcast.yaml"^
 "-Dhazelcast.jet.config=%HAZELCAST_HOME%\config\hazelcast-jet.yaml"^

set CLASSPATH="%HAZELCAST_HOME%\lib\*";%CLASSPATH%

ECHO ########################################
ECHO # RUN_JAVA=%RUN_JAVA%
ECHO # JAVA_OPTS=%JAVA_OPTS%
ECHO # CLASSPATH=%CLASSPATH%
ECHO ########################################
ECHO Starting Hazelcast

start "hazelcast" "%RUN_JAVA%" %JAVA_OPTS% -cp %CLASSPATH% com.hazelcast.core.server.HazelcastMemberStarter
goto endofscript

:error
ECHO JAVA_HOME not defined, cannot start the JVM

:endofscript

ENDLOCAL
