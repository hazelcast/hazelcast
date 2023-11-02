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

import org.apache.maven.repository.internal.MavenRepositorySystemUtils;
import org.eclipse.aether.artifact.DefaultArtifact;
import org.eclipse.aether.internal.impl.DefaultLocalRepositoryProvider;
import org.eclipse.aether.internal.impl.SimpleLocalRepositoryManagerFactory;
import org.eclipse.aether.repository.LocalRepository;
import org.eclipse.aether.repository.LocalRepositoryManager;
import org.eclipse.aether.repository.NoLocalRepositoryManagerException;

import com.hazelcast.internal.cluster.Versions;
import com.hazelcast.internal.tpcengine.util.OS;
import com.hazelcast.version.Version;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class HazelcastVersionLocator {
    public enum Artifact {
        OS_JAR(false, false, "hazelcast", "OS"),
        OS_TEST_JAR(false, true, "hazelcast", "OS tests"),
        SQL_JAR(false, false, "hazelcast-sql", "SQL"),
        EE_JAR(true, false, "hazelcast-enterprise", "EE");

        private static final String GROUP_ID = "com.hazelcast";
        private static final Path MAVEN_REPOSITORY;
        private static final LocalRepositoryManager REPOSITORY_MANAGER;

        static {
            try {
                final DefaultLocalRepositoryProvider repositoryProvider = new DefaultLocalRepositoryProvider();

                repositoryProvider.setLocalRepositoryManagerFactories(
                        Collections.singletonList(new SimpleLocalRepositoryManagerFactory()));

                // You can query this dynamically with the command:
                // mvn help:evaluate -Dexpression=settings.localRepository --quiet --batch-mode -DforceStdout
                //
                // But can be problematic to parse when additional VM arguments print logging information to stdout as well
                // https://github.com/hazelcast/hazelcast/issues/25451
                MAVEN_REPOSITORY = Paths.get(System.getProperty("user.home")).resolve(".m2").resolve("repository");

                if (Files.exists(MAVEN_REPOSITORY)) {
                    final LocalRepository localRepo = new LocalRepository(MAVEN_REPOSITORY.toFile());
                    REPOSITORY_MANAGER = repositoryProvider.newLocalRepositoryManager(MavenRepositorySystemUtils.newSession(),
                            localRepo);
                } else {
                    throw new NoSuchFileException(MAVEN_REPOSITORY.toString(), null, "Maven repository");
                }
            } catch (final IOException | NoLocalRepositoryManagerException e) {
                throw new RuntimeException(e);
            }
        }

        private final boolean enterprise;
        private final boolean test;
        private final String artifactId;
        private final String label;

        Artifact(final boolean enterprise, final boolean test, final String artifactId, final String label) {
            this.enterprise = enterprise;
            this.test = test;
            this.artifactId = artifactId;
            this.label = label;
        }

        private org.eclipse.aether.artifact.Artifact toAetherArtifact(final String version) {
            return new DefaultArtifact(GROUP_ID, artifactId, test ? "tests" : null, null, version);
        }

        /** @return a {@link Path} to the artifact in the local Maven repository, downloading if required */
        private Path locateArtifact(final String version) {
            final Path localCopy = MAVEN_REPOSITORY
                    .resolve(REPOSITORY_MANAGER.getPathForLocalArtifact(toAetherArtifact(version)) + ".jar");

            if (!Files.exists(localCopy)) {
                downloadArtifact(version);

                if (!Files.exists(localCopy)) {
                    throw new UncheckedIOException(new NoSuchFileException(localCopy.toString(), null,
                            MessageFormat.format("{0} (version \"{1}\") not found after download", this, version)));
                }
            }

            return localCopy;
        }

        private static String getMvn() {
            return OS.isWindows() ? "mvn.cmd" : "mvn";
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
                throw new RuntimeException(
                        MessageFormat.format("Problem in invoking Maven dependency:get {0}:{1}", this, version), e);
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

        @Override
        public String toString() {
            return label;
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
        return files.build()
                .collect(Collectors.toMap(Function.identity(), artifact -> artifact.locateArtifact(version).toFile()));
    }
}
