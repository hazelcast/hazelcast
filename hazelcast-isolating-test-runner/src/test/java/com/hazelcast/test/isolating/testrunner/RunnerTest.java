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

package com.hazelcast.test.isolating.testrunner;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.BindMode;
import org.testcontainers.containers.ContainerState;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

public class RunnerTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(RunnerTest.class);
    private static final int DEFAULT_RUNNERS_COUNT = Runtime.getRuntime().availableProcessors() / 2;
    private static final Path SORT_SEED_FILENAME = Paths.get("target/sort-seed");
    private final String testRunId = UUID.randomUUID().toString();

    @Test
    public void runHazelcastTestsInParallel() {
        int runnersCount = calculateRunnersCount();
        prepareTestBatches(runnersCount);
        runTestInDockerInstances(runnersCount);
    }

    private static int calculateRunnersCount() {
        return Optional.ofNullable(System.getProperty("runnersCount"))
                .map(Integer::parseInt)
                .orElse(DEFAULT_RUNNERS_COUNT);
    }

    private void runTestInDockerInstances(int runnersCount) {
        LOGGER.info("Starting " + runnersCount + " docker instances");

        List<GenericContainer<?>> containers = IntStream.range(0, runnersCount)
                .mapToObj(this::createContainer)
                .collect(Collectors.toList());
        await("docker instances have finished").atMost(Duration.ofMinutes(60))
                .until(() -> containers.stream().noneMatch(ContainerState::isRunning));
        assertThat(containers).withFailMessage("All containers should exit without error code")
                .allMatch(c -> Objects.equals(c.getCurrentContainerInfo().getState().getExitCodeLong(), 0L));
    }

    private GenericContainer<?> createContainer(int i) {
        char containerIdx = (char) ('a' + i);
        String shortName = "builder-" + containerIdx;
        String name = shortName + "-" + testRunId;
        String userId = getUserId();
        GenericContainer<?> mavenContainer = new GenericContainer<>("maven:3.9.2-eclipse-temurin-11")
                .withCreateContainerCmdModifier(cmd -> cmd.withName(name))
                .withCreateContainerCmdModifier(cmd -> cmd.getHostConfig().withCpuCount(8L))
                .withFileSystemBind("..", "/usr/src/maven", BindMode.READ_WRITE)
                .withFileSystemBind(System.getProperty("user.home") + "/.m2", "/root/.m2", BindMode.READ_WRITE)
                .withFileSystemBind("/var/run/docker.sock", "/var/run/docker.sock", BindMode.READ_WRITE)
                .withWorkingDirectory("/usr/src/maven")
                .withNetwork(newNetwork(name))
                .withCommand("bash", "-x", "-c", mvnCommandForBatch(containerIdx));
        if (userId != null) {
            mavenContainer.withCreateContainerCmdModifier(cmd -> cmd.withUser(userId));
        }
        mavenContainer.start();
        mavenContainer.followOutput(new Slf4jLogConsumer(LOGGER).withPrefix(shortName));
        return mavenContainer;
    }

    private static String getUserId() {
        try {
            Process exec = exec("id -u");
            return new BufferedReader(new InputStreamReader(exec.getInputStream())).readLine();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Network newNetwork(String name) {
        return Network.builder()
                .createNetworkCmdModifier(cmd -> cmd.withName(name))
                .build();
    }

    private static String mvnCommandForBatch(char batchSuffix) {
        String listOfTests = "/usr/src/maven/hazelcast-isolating-test-runner/target/test-batch-" + batchSuffix;
        String sharedProjectDir = "/usr/src/maven";
        String isolatedProjectDir = "/usr/src/maven-isolated";
        String sharedSurefireReports = sharedProjectDir + "/hazelcast/target/surefire-reports";
        return "cp -R " + sharedProjectDir + "/ " + isolatedProjectDir + "; cd " + isolatedProjectDir + ";"
                + "mvn --errors surefire:test --fail-at-end -Ppr-builder -Ponly-explicit-tests -pl hazelcast "
                + "-Dsurefire.includesFile=" + listOfTests + " -Dbasedir=test-batch-" + batchSuffix + "-dir;"
                + "mkdir -p " + sharedSurefireReports + ";"
                + "cp -v " + isolatedProjectDir + "/hazelcast/target/surefire-reports/* " + sharedSurefireReports;
    }

    private static void prepareTestBatches(int runnersCount) {
        String listAllTestClassesCommand = "find ../hazelcast/src/test/java -name '*.java' | sort | cut -sd / -f 6-";
        int totalNumberOfTests = countOutputLines(listAllTestClassesCommand);
        LOGGER.info("Found " + totalNumberOfTests + " tests to run");
        int testCountInBatch = totalNumberOfTests / runnersCount + (totalNumberOfTests % runnersCount == 0 ? 0 : 1);
        String prepareTestBatchesCommand = listAllTestClassesCommand + " | sort -R --random-source=" + generateSeedFile() + " | split -l " + testCountInBatch + " -a 1 - target/test-batch-";
        exec(prepareTestBatchesCommand);
    }

    private static Path generateSeedFile() {
        String sortSeed = Optional.ofNullable(System.getProperty("sortSeed"))
                .orElse(UUID.randomUUID().toString()).substring(0, 32);
        LOGGER.info("Sorting seed: " + sortSeed + ". Use -DsortSeed=" + sortSeed + " to re-run tests with the same order");
        byte[] bytes = sortSeed.getBytes(StandardCharsets.UTF_8);
        try {
            return Files.write(SORT_SEED_FILENAME, bytes, TRUNCATE_EXISTING, CREATE);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static Process exec(String command) {
        try {
            String[] cmd = {"/bin/sh", "-c", command};
            Process process = Runtime.getRuntime().exec(cmd);
            process.waitFor();
            return process;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static int countOutputLines(String command) {
        Process process = exec(command + " | wc -l");
        BufferedReader buf = new BufferedReader(new InputStreamReader(process.getInputStream()));
        try {
            return Integer.parseInt(buf.readLine().trim());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
