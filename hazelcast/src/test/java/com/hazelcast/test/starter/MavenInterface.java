/*
 * Copyright (c) 2008-2024, Hazelcast, Inc. All Rights Reserved.
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

import org.apache.commons.io.FilenameUtils;
import org.apache.commons.io.IOUtils;
import org.apache.maven.repository.internal.MavenRepositorySystemUtils;
import org.eclipse.aether.artifact.Artifact;
import org.eclipse.aether.internal.impl.DefaultLocalPathComposer;
import org.eclipse.aether.internal.impl.DefaultLocalRepositoryProvider;
import org.eclipse.aether.internal.impl.SimpleLocalRepositoryManagerFactory;
import org.eclipse.aether.repository.LocalRepository;
import org.eclipse.aether.repository.LocalRepositoryManager;
import org.eclipse.aether.repository.NoLocalRepositoryManagerException;
import org.h2.util.StringUtils;

import com.hazelcast.internal.tpcengine.util.OS;
import com.hazelcast.internal.util.StringUtil;
import com.hazelcast.jet.impl.util.ConcurrentMemoizingSupplier;

import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

public class MavenInterface {
    /** The path to the {@code mvn} executable */
    private static final ConcurrentMemoizingSupplier<String> MVN =
            new ConcurrentMemoizingSupplier<>(() -> {
                Path path = Paths.get(".")
                        .toAbsolutePath();

                // Recurse upwards until you find the Maven wrapper
                while (path != null) {
                    Path mvnPath = path.resolve(getMvnCommand());

                    if (Files.exists(mvnPath)) {
                        return mvnPath.toString();
                    } else {
                        path = path.getParent();
                    }
                }

                throw new UncheckedIOException(new NoSuchFileException("Could not find Maven wrapper"));
            });
    private static final Path MAVEN_REPOSITORY;
    private static final LocalRepositoryManager REPOSITORY_MANAGER;

    static {
        try {
            final DefaultLocalRepositoryProvider repositoryProvider = new DefaultLocalRepositoryProvider(
                    Collections.singleton(new SimpleLocalRepositoryManagerFactory(new DefaultLocalPathComposer())));

            MAVEN_REPOSITORY = Paths.get(evaluateExpression("settings.localRepository"));

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

    /** @return a {@link Path} to the artifact in the local Maven repository, downloading if required */
    public static Path locateArtifact(Artifact artifact, String... remoteRepositories) {
        final Path localCopy = MAVEN_REPOSITORY
                .resolve(REPOSITORY_MANAGER.getPathForLocalArtifact(artifact) + FilenameUtils.EXTENSION_SEPARATOR
                        + (StringUtils.isNullOrEmpty(artifact.getExtension()) ? "jar" : artifact.getExtension()));

        if (!Files.exists(localCopy)) {
            downloadArtifact(artifact, remoteRepositories);

            if (!Files.exists(localCopy)) {
                throw new UncheckedIOException(new NoSuchFileException(localCopy.toString(), null,
                        MessageFormat.format("{0} not found after download", artifact)));
            }
        }

        return localCopy;
    }

    private static String getMvnCommand() {
        return OS.isWindows() ? "mvnw.cmd" : "mvnw";
    }

    private static void downloadArtifact(Artifact artifact, String... remoteRepositories) {
        // It's also possible to download this via Aether but I was unable to get this working
        final ProcessBuilder builder = new ProcessBuilder(
                buildMavenCommand(artifact, remoteRepositories).toArray(String[]::new)).inheritIO();
        configureMavenEnvironment(builder);

        try {
            int exitValue = builder.start().waitFor();
            checkState(exitValue == 0, MessageFormat.format("Maven dependency:get failed with code {0}", exitValue));
        } catch (InterruptedException | IOException e) {
            throw new RuntimeException(MessageFormat.format("Problem in invoking Maven dependency:get {0}", artifact), e);
        }
    }

    private static Stream<String> buildMavenCommand(Artifact artifact, String... remoteRepositories) {
        final Stream.Builder<String> builder = Stream.builder();

        builder.add(MVN.get());

        builder.add("dependency:get");
        builder.add("-DgroupId=" + artifact.getGroupId());
        builder.add("-DartifactId=" + artifact.getArtifactId());
        builder.add("-Dversion=" + artifact.getVersion());

        if (!StringUtil.isNullOrEmpty(artifact.getClassifier())) {
            builder.add("-Dclassifier=" + artifact.getClassifier());
        }

        if (remoteRepositories.length != 0) {
            builder.add("-DremoteRepositories=" + String.join(",", remoteRepositories));
        }

        return builder.build();
    }

    private static void configureMavenEnvironment(ProcessBuilder processBuilder) {
        // Overrides any existing "MAVEN_OPTS" environment configuration
        //
        // This is to ignore any custom configuration of the parent Maven invocation that will interfere with this simple query
        //
        // https://github.com/hazelcast/hazelcast/issues/25451#issuecomment-1720248676/
        // https://github.com/hazelcast/hazelcast-mono/issues/3017#issuecomment-2301777642
        processBuilder.environment()
                .remove("MAVEN_OPTS");
        processBuilder.environment()
                .put("MAVEN_ARGS", "--quiet --batch-mode");
    }

    /** @return a {@link String} output of {@code mvn help:evaluate -Dexpression=EXPRESSION} */
    public static String evaluateExpression(String expression) throws IOException {
        // Ideally you'd run this using the maven-invoker plugin, but I couldn't get this to work -
        // https://stackoverflow.com/q/76866880
        // We use `--quiet` to only output the expression result
        ProcessBuilder processBuilder =
                new ProcessBuilder(MVN.get(), "help:evaluate", "--quiet", "-Dexpression=" + expression, "-DforceStdout");
        configureMavenEnvironment(processBuilder);

        try (InputStream stream = processBuilder.start()
                .getInputStream()) {
            List<String> output = IOUtils.readLines(stream, StandardCharsets.UTF_8);

            if (output.size() == 1) {
                return output.get(0);
            } else {
                throw new IOException("Maven expression (\"%s\") returned unexpected response:%n%s".formatted(expression,
                        String.join(System.lineSeparator(), output)));
            }
        }
    }
}
