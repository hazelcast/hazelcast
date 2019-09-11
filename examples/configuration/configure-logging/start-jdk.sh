#!/bin/sh

java -cp target/lib/*:target/classes -Dhazelcast.logging.type=jdk  JetInstance
