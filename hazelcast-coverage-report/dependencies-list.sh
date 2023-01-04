#!/bin/bash
mvn -f .. -T 1 -B exec:exec -Dexec.executable=echo -Dexec.args='#Module-info:<dependency><groupId>${project.groupId}</groupId><artifactId>${project.artifactId}</artifactId><version>$${project.version}</version><type>pom</type></dependency>'| grep '#Module-info:' | grep -v 'hazelcast-coverage-report' | cut -f2 -d':'
