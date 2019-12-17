@echo off

REM ******* This script kills all the instances started with start.bat script.

taskkill /F /FI "WINDOWTITLE eq hazelcast-imdg"
