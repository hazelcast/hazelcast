/*
 * Copyright (c) 2008-2021, Hazelcast, Inc. All Rights Reserved.
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
package com.hazelcast.distribution;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.test.HazelcastParallelClassRunner;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.test.HazelcastTestSupport.assumeThatNoWindowsOS;
import static io.restassured.RestAssured.given;
import static org.apache.http.HttpStatus.SC_OK;
import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

@RunWith(HazelcastParallelClassRunner.class)
@Category(QuickTest.class)
public class DistributionIT {
    private static final String BASE_NAME = "hazelcast-" + getHzVersion();
    private static final String DISTRIBUTION_FILE = "target/" + BASE_NAME + ".tar.gz";

    @Rule
    public TemporaryFolder temporaryFolder = new TemporaryFolder();

    private Process hzProcess;
    private String hazelcastHome;

    @Test
    public void should_start_instance_with_hz_command() throws Exception {
        assumeThatNoWindowsOS();
        String tmpDirectory = temporaryFolder.newFolder().getAbsolutePath();
        untarDistribution(tmpDirectory);
        hazelcastHome = tmpDirectory + "/" + BASE_NAME;

        String[] startInstanceCmd = {hazelcastHome + "/bin/hz", "start", "-c", "src/test/resources/integration-test-hazelcast.yaml"};
        hzProcess = executeAsync(startInstanceCmd);

        await().atMost(30, TimeUnit.SECONDS)
                .ignoreExceptions()
                .until(DistributionIT::memberIsHealthy);
    }

    private static boolean memberIsHealthy() {
        int port = getFirstMemberPort();
        return given().baseUri("http://127.0.0.1:" + port + "/hazelcast/health/ready")
                .when().get()
                .then().extract().statusCode() == SC_OK;
    }

    private static int getFirstMemberPort() {
        ClientConfig clientConfig = new ClientConfig();
        clientConfig.setClusterName("hz-it-cluster");
        clientConfig.getNetworkConfig().addAddress("127.0.0.1");
        HazelcastInstance client = HazelcastClient.newHazelcastClient(clientConfig);
        try {
            Member firstMember = client.getCluster().getMembers().iterator().next();
            return firstMember.getAddress().getPort();
        } finally {
            client.shutdown();
        }
    }

    private static String getHzVersion() {
        Properties properties = new Properties();
        try (FileInputStream fileInputStream = new FileInputStream("target/maven-archiver/pom.properties")) {
            properties.load(fileInputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return properties.getProperty("version");
    }

    private static void untarDistribution(String targetDirectory) throws InterruptedException, IOException {
        int unpackingExitCode = execute("tar", "-xzvf", DISTRIBUTION_FILE, "-C", targetDirectory);
        assertThat(unpackingExitCode).describedAs("untar should be successful").isZero();
    }

    private static int execute(String... commandAndArgs) throws InterruptedException, IOException {
        return executeAsync(commandAndArgs).waitFor();
    }

    private static Process executeAsync(String... commandAndArgs) throws IOException {
        ProcessBuilder processBuilder = new ProcessBuilder(commandAndArgs);
        Map<String, String> environment = processBuilder.environment();
        environment.put("JAVA_OPTS", "-Dhazelcast.phone.home.enabled=false");
        environment.put("JAVA_HOME", System.getProperty("java.home")); //workaround for IntelliJ on Mac
        return processBuilder.redirectErrorStream(true).inheritIO().start();
    }

    @After
    public void tearDown() throws Exception {
        if (hazelcastHome != null) {
            execute(hazelcastHome + "/bin/hz-stop");
        }
        if (hzProcess != null) {
            hzProcess.destroy();
        }
    }
}
