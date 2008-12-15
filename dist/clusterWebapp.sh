#!/bin/sh

# Usage: clusterWebapp.sh <your-ear-war-file>
# e.g  : clusterWebapp.sh myapp.ear
# e.g  : clusterWebapp.sh mywebapp.war

java -cp hazelcast.jar com.hazelcast.web.Installer $*