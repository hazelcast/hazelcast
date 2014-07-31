#!/bin/sh

java -server -Djava.net.preferIPv4Stack=true -cp ../lib/hazelcast-client-${project.version}.jar:../lib/hazelcast-${project.version}.jar com.hazelcast.client.examples.ClientTestApp