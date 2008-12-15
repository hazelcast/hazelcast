#!/bin/sh

# javac -cp hazelcast.jar -d testapp ./testapp/TestApp.java

java -Djava.net.preferIPv4Stack=true -cp ./testapp:hazelcast.jar TestApp

