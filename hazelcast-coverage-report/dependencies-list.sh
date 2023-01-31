#!/bin/bash
#
# Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

#### IMPORTANT NOTE:
# Output of this command may differ depending on used JDK version - it prints only active modules (not all modules).
# Therefore, it is required to run that on the same JDK as sonar build.
mvn -f .. -T 1 -B exec:exec -Dexec.executable=echo -Dexec.args='#Module-info:<dependency><groupId>${project.groupId}</groupId><artifactId>${project.artifactId}</artifactId><version>$${project.version}</version><type>pom</type></dependency>' | grep '#Module-info:' | cut -f2 -d':'
