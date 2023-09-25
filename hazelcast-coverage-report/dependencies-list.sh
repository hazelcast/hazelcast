#!/bin/bash
#### IMPORTANT NOTE:
# Output of this command may differ depending on used JDK version - it prints only active modules (not all modules).
# Therefore, it is required to run that on the same JDK as sonar build.
mvn -f .. -T 1 -B exec:exec -Dexec.executable=echo -Dexec.args='#Module-info:<dependency><groupId>${project.groupId}</groupId><artifactId>${project.artifactId}</artifactId><version>$${project.version}</version><type>pom</type></dependency>' | grep '#Module-info:' | cut -f2 -d':'
