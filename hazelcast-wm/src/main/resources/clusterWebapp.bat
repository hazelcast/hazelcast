@ECHO OFF

@REM Usage: clusterWebapp.bat <your-ear-war-file>
@REM e.g  : clusterWebapp.bat myapp.ear
@REM e.g  : clusterWebapp.bat mywebapp.war

java -cp ../lib/hazelcast-${project.version}.jar:../lib/hazelcast-wm-${project.version}.jar com.hazelcast.web.Installer %*