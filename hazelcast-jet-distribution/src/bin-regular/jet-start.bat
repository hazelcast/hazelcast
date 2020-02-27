@echo off

SETLOCAL

if NOT DEFINED JAVA_HOME goto error
set RUN_JAVA=%JAVA_HOME%\bin\java
set JET_HOME=%~dp0..

set JAVA_OPTS=%JAVA_OPTS%^
 "-Dhazelcast.logging.type=log4j2"^
 "-Dlog4j.configurationFile=file:%JET_HOME%\config\log4j2.properties"^
 "-Dhazelcast.config=%JET_HOME%\config\hazelcast.yaml"^
 "-Dhazelcast.jet.config=%JET_HOME%\config\hazelcast-jet.yaml"^
 "-Djet.home=%JET_HOME%"

set CLASSPATH="%JET_HOME%\lib\*";%CLASSPATH%

ECHO ########################################
ECHO # RUN_JAVA=%RUN_JAVA%
ECHO # JAVA_OPTS=%JAVA_OPTS%
ECHO # CLASSPATH=%CLASSPATH%
ECHO ########################################
ECHO Starting Hazelcast Jet

start "hazelcast-jet" "%RUN_JAVA%" %JAVA_OPTS% -cp %CLASSPATH% com.hazelcast.jet.server.JetMemberStarter
goto endofscript

:error
ECHO JAVA_HOME not defined, cannot start the JVM

:endofscript

ENDLOCAL
