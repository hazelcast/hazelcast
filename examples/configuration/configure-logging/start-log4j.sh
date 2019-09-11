#!/bin/sh

#add '-Dlog4j.debug' if logging doesn't work
java -cp target/lib/*:target/classes -Dhazelcast.logging.type=log4j  JetInstance
