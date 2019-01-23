@echo off

SETLOCAL

if NOT DEFINED JAVA_HOME goto error
set RUN_JAVA=%JAVA_HOME%\bin\java
set JET_HOME="%~dp0.."
set CLASSPATH=%JET_HOME%\lib\${hazelcast.jet.artifact}-${project.version}.jar;%CLASSPATH%
set JAVA_OPTS=%JAVA_OPTS% -Dhazelcast.client.config=%JET_HOME%\config\hazelcast-client.xml

"%RUN_JAVA%" %JAVA_OPTS% -cp %CLASSPATH% com.hazelcast.jet.server.JetCommandLine %*
goto endofscript

:error
echo JAVA_HOME environment variable must be set!
pause

:endofscript

ENDLOCAL
