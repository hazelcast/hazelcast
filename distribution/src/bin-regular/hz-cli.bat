@echo off

SETLOCAL

if NOT DEFINED JAVA_HOME goto error
set RUN_JAVA=%JAVA_HOME%\bin\java
set HAZELCAST_HOME=%~dp0..
set CLASSPATH="%HAZELCAST_HOME%\lib\*";%CLASSPATH%
set JAVA_OPTS=%JAVA_OPTS% "-Dhazelcast.client.config=%HAZELCAST_HOME%\config\hazelcast-client.yaml"

"%RUN_JAVA%" %JAVA_OPTS% -cp %CLASSPATH% com.hazelcast.client.console.HazelcastCommandLine %*
goto endofscript

:error
echo JAVA_HOME not defined, cannot start the JVM
pause

:endofscript

ENDLOCAL
