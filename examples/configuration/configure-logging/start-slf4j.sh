#!/bin/sh

java -cp target/lib/*:target/classes -Dhazelcast.logging.type=slf4j JetInstance
