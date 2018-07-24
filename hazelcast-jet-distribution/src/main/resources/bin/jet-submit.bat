@echo off

SETLOCAL

if NOT DEFINED JAVA_HOME goto error
set RUN_JAVA=%JAVA_HOME%\bin\java

set CLASSPATH=%~dp0..\lib\${hazelcast.jet.artifact}-${project.version}.jar;%CLASSPATH%
SET JAR_FILE=%1

ECHO ########################################
ECHO # RUN_JAVA=%RUN_JAVA%
ECHO # CLASSPATH=%CLASSPATH%
ECHO # starting now....
ECHO ########################################

"%RUN_JAVA%" -cp "%CLASSPATH%";"%JAR_FILE%" com.hazelcast.jet.server.JetBootstrap %*
goto endofscript 

:error
ECHO JAVA_HOME environment variable must be set!
pause


:endofscript

ENDLOCAL 