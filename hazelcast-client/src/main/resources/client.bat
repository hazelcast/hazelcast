@ECHO OFF

java -Djava.net.preferIPv4Stack=true -classpath ../lib/hazelcast-${project.version}.jar:../lib/hazelcast-client-${project.version}.jar com.hazelcast.client.examples.TestClientApp