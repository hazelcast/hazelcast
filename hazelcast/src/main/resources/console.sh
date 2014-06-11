#!/bin/sh

java -server -Djava.net.preferIPv4Stack=true -cp ../lib/hazelcast-${project.version}.jar com.hazelcast.console.ConsoleApp

