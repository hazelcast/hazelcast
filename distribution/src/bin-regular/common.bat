
:: detect Java

if "x%JAVA_HOME%" == "x" (
    echo JAVA_HOME is not set, running Java as 'java'
    set RUN_JAVA=java
) else (
    echo JAVA_HOME is set, running Java at '%JAVA_HOME%\bin\java'
    set "RUN_JAVA=%JAVA_HOME%\bin\java"
)

"%RUN_JAVA%" -version 1>nul 2>nul || (
    echo JAVA could not be found in your system.
    echo Please install Java 1.8 or higher and ensure either JAVA_HOME
    echo is defined, or the java executable is in the PATH.
    exit /b 2
)

:: %~dp0 is the drive+path where this script lives
if "x%HAZELCAST_HOME%" == "x" (
    echo HAZELCAST_HOME is not set, expecting Hazelcast at '%~dp0..'
    set HAZELCAST_HOME=%~dp0..
) else (
    echo HAZELCAST_HOME is set, expecting Hazelcast at '%HAZELCAST_HOME%'
)

:: options

:: ******* you can enable following variables by uncommenting them

:: ******* minimum heap size
:: set MIN_HEAP_SIZE=1G

:: ******* maximum heap size
:: set MAX_HEAP_SIZE=1G


if NOT "%MIN_HEAP_SIZE%" == "" (
	set JAVA_OPTS=%JAVA_OPTS% -Xms%MIN_HEAP_SIZE%
)

if NOT "%MAX_HEAP_SIZE%" == "" (
	set JAVA_OPTS=%JAVA_OPTS% -Xmx%MAX_HEAP_SIZE%
)

:: options

FOR /F "tokens=* USEBACKQ" %%F IN (`CALL "%RUN_JAVA%" -cp "%HAZELCAST_HOME%\lib\*" com.hazelcast.internal.util.JavaVersion`) DO SET JAVA_VERSION=%%F

IF %JAVA_VERSION% GEQ 9 (
    SET JAVA_OPTS=%JAVA_OPTS%^
        --add-modules java.se^
        --add-exports java.base/jdk.internal.ref=ALL-UNNAMED^
        --add-opens java.base/java.lang=ALL-UNNAMED^
        --add-opens java.base/sun.nio.ch=ALL-UNNAMED^
        --add-opens java.management/sun.management=ALL-UNNAMED^
        --add-opens jdk.management/com.sun.management.internal=ALL-UNNAMED

    FOR /F "tokens=2 delims== USEBACKQ" %%A IN (`CALL "%RUN_JAVA%" -XshowSettings:properties -version 2^>^&1 ^| findstr /c:"java.vm.name"`) DO SET VM_NAME=%%A

    :: trim leading ws, trailing ws
    FOR /F "tokens=* delims= " %%A IN ("!VM_NAME!") DO SET VM_NAME=%%~A
    FOR /L %%a in (1,1,15) DO IF "!VM_NAME:~-1!"==" " SET VM_NAME=!VM_NAME:~0,-1!

    IF NOT "x!VM_NAME:OpenJ9=!" == "x!VM_NAME!" (
    	REM OpenJ9 detected, adding additional exports
    	set JAVA_OPTS=%JAVA_OPTS% --add-exports jdk.management/com.ibm.lang.management.internal=ALL-UNNAMED
    )
)

:: HAZELCAST_CONFIG holds path to the configuration file. The path is relative to the Hazelcast installation (HAZELCAST_HOME).
if "x%HAZELCAST_CONFIG%" == "x" (
    set HAZELCAST_CONFIG=config\hazelcast.xml
    if not exist "%HAZELCAST_HOME%\!HAZELCAST_CONFIG!" (
      set HAZELCAST_CONFIG=config\hazelcast.yaml
    )
    if not exist "%HAZELCAST_HOME%\!HAZELCAST_CONFIG!" (
      set HAZELCAST_CONFIG=config\hazelcast.yml
    )
    if not exist "%HAZELCAST_HOME%\!HAZELCAST_CONFIG!" (
      echo "Configuration file is missing. Create hazelcast.[xml|yaml|yml] in %HAZELCAST_HOME%\config or set the HAZELCAST_CONFIG environment variable."
      exit /b 2
    )
    echo HAZELCAST_CONFIG is not set, using '%HAZELCAST_HOME%\!HAZELCAST_CONFIG!'
) else (
    echo HAZELCAST_CONFIG is set, using configuration file at '!HAZELCAST_CONFIG!'
)

set JAVA_OPTS=%JAVA_OPTS%^
    -Dhazelcast.config="%HAZELCAST_HOME%\!HAZELCAST_CONFIG!"

:: classpath

set CLASSPATH="%HAZELCAST_HOME%\lib\*;%HAZELCAST_HOME%\bin\user-lib;%HAZELCAST_HOME%\bin\user-lib\*;%CLASSPATH%"