@echo off

REM ******* This script kills java processes running HazelcastMemberStarter

wmic process where "Name like '%%java%%' AND CommandLine like '%%HazelcastMemberStarter%%'" delete
