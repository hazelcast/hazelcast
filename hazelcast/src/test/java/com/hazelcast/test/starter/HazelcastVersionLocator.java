/*
 * Copyright (c) 2008-2023, Hazelcast, Inc. All Rights Reserved.
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

import static com.hazelcast.internal.util.Preconditions.checkState;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UncheckedIOException;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.maven.artifact.DefaultArtifact;
import org.apache.maven.artifact.handler.ArtifactHandler;
import org.apache.maven.artifact.handler.DefaultArtifactHandler;
import org.apache.maven.artifact.repository.layout.ArtifactRepositoryLayout;
import org.apache.maven.artifact.repository.layout.DefaultRepositoryLayout;

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.util.OsHelper;
import com.hazelcast.version.Version;

public class HazelcastVersionLocator {
    public enum Artifact {
        OS_JAR(false, false, "hazelcast"),
        OS_TEST_JAR(false, true, "hazelcast"),
        SQL_JAR(false, false, "hazelcast-sql"),
        EE_JAR(true, false, "hazelcast-enterprise");

        private static final String GROUP_ID = "com.hazelcast";
        private static final ArtifactHandler ARTIFACT_HANDLER = new DefaultArtifactHandler("");
        private static final ArtifactRepositoryLayout REPOSITORY_LAYOUT = new DefaultRepositoryLayout();

        private final boolean enterprise;
        private final boolean test;
        private final String artifactId;

        Artifact(final boolean enterprise, final boolean test, final String artifactId) {
            this.enterprise = enterprise;
            this.test = test;
            this.artifactId = artifactId;
        }

        private org.apache.maven.artifact.Artifact toMavenArtifact(final String version) {
            return new DefaultArtifact(GROUP_ID, artifactId, version, "", "", test ? "tests" : "", ARTIFACT_HANDLER);
        }

        /** @return a path to the artifact in the local Maven repository, downloading if required */
        private File locateArtifact(final String version) {
            final File localCopy = new File(
                    LOCAL_M2_REPOSITORY_PREFIX + File.separator + REPOSITORY_LAYOUT.pathOf(toMavenArtifact(version)) + ".jar");

            if (!localCopy.exists()) {
                downloadArtifact(version);
            }
            return localCopy;
        }

        private void downloadArtifact(final String version) {
            final ProcessBuilder builder = new ProcessBuilder(buildMavenCommand(version).toArray(String[]::new)).inheritIO();
            try {
                final Process process = builder.start();
                final boolean successful = process.waitFor(2, TimeUnit.MINUTES);
                checkState(successful, "Maven dependency:get timed out");
                checkState(process.exitValue() == 0, "Maven dependency:get failed");
            } catch (InterruptedException | IOException e) {
                throw new RuntimeException("Problem in invoking Maven dependency:get " + toString() + ":" + version, e);
            }
        }

        private Stream<String> buildMavenCommand(final String version) {
            final Stream.Builder<String> builder = Stream.builder();

            builder.add(getMvn());

            builder.add("dependency:get");
            builder.add("-DgroupId=" + GROUP_ID);
            builder.add("-DartifactId=" + artifactId);
            builder.add("-Dversion=" + version);

            if (test) {
                builder.add("-Dclassifier=tests");
            }

            if (enterprise) {
                builder.add("-DremoteRepositories=https://repository.hazelcast.com/release");
            }

            return builder.build();
        }
    }

    private static final String LOCAL_M2_REPOSITORY_PREFIX;

    static {
        try {
            // https://stackoverflow.com/a/16218772
            // Ideally you'd run this using the maven-invoker plugin, but I couldn't get this to work -
            // https://stackoverflow.com/q/76866880
            final Process process = new ProcessBuilder(getMvn(), "help:evaluate", "-Dexpression=settings.localRepository",
                    "--quiet", "--batch-mode", "-DforceStdout").start();

            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                LOCAL_M2_REPOSITORY_PREFIX = reader.readLine();
            }
        } catch (final IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String getMvn() {
        if (OsHelper.isWindows()) {
            return "mvn.cmd";
        } else {
            final String mvn = "mvn";

            if (!new File(mvn).exists() && OsHelper.isMac()) {
                // Eclipse doesn't properly read the $PATH, so hardcode another location - https://stackoverflow.com/q/76866453
                return "/opt/homebrew/bin/mvn";
            }

            return mvn;
        }
    }

    public static Map<Artifact, File> locateVersion(final String version, final boolean enterprise) {
        final Stream.Builder<Artifact> files = Stream.builder();
        files.add(Artifact.OS_JAR);
        files.add(Artifact.OS_TEST_JAR);
        if (Version.of(version).isGreaterOrEqual(Versions.V5_0)) {
            files.add(Artifact.SQL_JAR);
        }
        if (enterprise) {
            files.add(Artifact.EE_JAR);
        }
        return files.build().collect(Collectors.toMap(Function.identity(), artifact -> artifact.locateArtifact(version)));
    }
}
