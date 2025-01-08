/*
 * Copyright (c) 2008-2025, Hazelcast, Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.test.starter;

import org.eclipse.aether.artifact.DefaultArtifact;

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.version.Version;

import java.io.File;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HazelcastVersionLocator {
    private static final String GROUP_ID = "com.hazelcast";

    public enum Artifact {
        OS_JAR(false, false, "hazelcast"),
        OS_TEST_JAR(false, true, "hazelcast"),
        SQL_JAR(false, false, "hazelcast-sql"),
        EE_JAR(true, false, "hazelcast-enterprise");

        private final boolean enterprise;
        private final boolean test;
        private final String artifactId;

        Artifact(final boolean enterprise, final boolean test, final String artifactId) {
            this.enterprise = enterprise;
            this.test = test;
            this.artifactId = artifactId;
        }

        private org.eclipse.aether.artifact.Artifact toAetherArtifact(final String version) {
            return new DefaultArtifact(GROUP_ID, artifactId, test ? "tests" : null, null, version);
        }
    }

    public static Map<Artifact, File> locateVersion(final String version, final boolean enterprise) {
        final Stream.Builder<Artifact> files = Stream.builder();
        if (Version.of(version).isGreaterOrEqual(Versions.V5_0)) {
            files.add(Artifact.SQL_JAR);
        }
        if (enterprise) {
            files.add(Artifact.EE_JAR);
        } else {
            files.add(Artifact.OS_JAR);
            files.add(Artifact.OS_TEST_JAR);
        }
        return files.build().collect(Collectors.toMap(Function.identity(),
                artifact -> MavenInterface.locateArtifact(artifact.toAetherArtifact(version),
                        artifact.enterprise ? new String[] {"https://repository.hazelcast.com/release"} : new String[] {})
                        .toFile()));
    }
}
