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
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.maven.repository.internal.MavenRepositorySystemUtils;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.internal.impl.DefaultLocalRepositoryProvider;
import org.eclipse.aether.internal.impl.SimpleLocalRepositoryManagerFactory;
import org.eclipse.aether.repository.LocalRepository;
import org.eclipse.aether.repository.LocalRepositoryManager;
import org.eclipse.aether.repository.NoLocalRepositoryManagerException;

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
        private static final LocalRepositoryManager REPOSITORY_MANAGER;

        static {
            try {
                // https://stackoverflow.com/a/16218772
                // Ideally you'd run this using the maven-invoker plugin, but I couldn't get this to work -
                // https://stackoverflow.com/q/76866880
                final Process process = new ProcessBuilder(getMvn(), "help:evaluate", "-Dexpression=settings.localRepository",
                        "--quiet", "--batch-mode", "-DforceStdout").start();

                try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                    final DefaultLocalRepositoryProvider repositoryProvider = new DefaultLocalRepositoryProvider();

                    repositoryProvider.setLocalRepositoryManagerFactories(
                            Collections.singletonList(new SimpleLocalRepositoryManagerFactory()));

                    final LocalRepository localRepo = new LocalRepository(reader.readLine());
                    REPOSITORY_MANAGER = repositoryProvider.newLocalRepositoryManager(MavenRepositorySystemUtils.newSession(),
                            localRepo);
                }
            } catch (final IOException | NoLocalRepositoryManagerException e) {
                throw new RuntimeException(e);
            }
        }

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

        /** @return a path to the artifact in the local Maven repository, downloading if required */
        private File locateArtifact(final String version) {
            final File localCopy = new File(REPOSITORY_MANAGER.getRepository().getBasedir(),
                    REPOSITORY_MANAGER.getPathForLocalArtifact(toAetherArtifact(version)) + ".jar");

            if (!localCopy.exists()) {
                downloadArtifact(version);
            }
            return localCopy;
        }

        private static String getMvn() {
            return OsHelper.isWindows() ? "mvn.cmd" : "mvn";
        }

        private void downloadArtifact(final String version) {
            // It's also possible to download this via Aether but I was unable to get this working
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
            builder.add("--quiet");
            builder.add("--batch-mode");

            if (test) {
                builder.add("-Dclassifier=tests");
            }

            if (enterprise) {
                builder.add("-DremoteRepositories=https://repository.hazelcast.com/release");
            }

            return builder.build();
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
