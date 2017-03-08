@echo off

SETLOCAL

set "CLASSPATH=%~dp0..\lib\hazelcast-all-${project.version}.jar"

taskkill /F /FI "WINDOWTITLE eq hazelcast %CLASSPATH%"
