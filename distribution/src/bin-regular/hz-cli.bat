@echo off

SETLOCAL ENABLEDELAYEDEXPANSION

CALL common.bat

echo "%RUN_JAVA%" %JAVA_OPTS% -cp %CLASSPATH% com.hazelcast.client.console.HazelcastCommandLine %*
"%RUN_JAVA%" %JAVA_OPTS% -cp %CLASSPATH% com.hazelcast.client.console.HazelcastCommandLine %*

ENDLOCAL
