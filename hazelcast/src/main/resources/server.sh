#!/bin/sh

java -server -Xms1G -Xmx1G -Djava.net.preferIPv4Stack=true -cp ../lib/hazelcast-${project.version}.jar com.hazelcast.core.server.StartServer


