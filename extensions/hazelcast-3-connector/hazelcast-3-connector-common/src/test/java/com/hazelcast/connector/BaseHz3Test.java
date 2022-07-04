/*
 * Copyright 2021 Hazelcast Inc.
 *
 * Licensed under the Hazelcast Community License (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://hazelcast.com/hazelcast-community-license
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.hazelcast.connector;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.jet.config.JobConfig;
import com.hazelcast.spi.properties.ClusterProperty;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.starter.hz3.Hazelcast3Starter;
import org.junit.After;
import org.junit.Before;

import javax.annotation.Nonnull;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class BaseHz3Test extends HazelcastTestSupport {

    // Configuration of the member that's used to start the remote Hz 3 cluster - uses 3.x schema
    protected static final String HZ3_MEMBER_CONFIG =
            "<hazelcast xmlns=\"http://www.hazelcast.com/schema/config\"\n" +
            "           xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n" +
            "           xsi:schemaLocation=\"http://www.hazelcast.com/schema/config\n" +
            "           http://www.hazelcast.com/schema/config/hazelcast-config-3.12.xsd\">\n" +
            "    <group>\n" +
            "        <name>dev</name>\n" +
            "    </group>\n" +
            "    <network>\n" +
            "        <port auto-increment=\"true\" port-count=\"100\">3210</port>\n" +
            "        <join>\n" +
            "            <multicast enabled=\"false\">\n" +
            "            </multicast>\n" +
            "        </join>\n" +
            "    </network>\n" +
            "</hazelcast>\n";

    // Client configuration used the source to connect to the remote Hz 3 cluster - uses 3.x schema
    protected static final String HZ3_CLIENT_CONFIG =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<hazelcast-client xmlns=\"http://www.hazelcast.com/schema/client-config\"\n"
            + "                  xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
            + "                  xsi:schemaLocation=\"http://www.hazelcast.com/schema/client-config\n"
            + "                  http://www.hazelcast.com/schema/client-config/hazelcast-client-config-3.12.xsd\">\n"
            + "\n"
            + "    <network>\n"
            + "        <cluster-members>\n"
            + "            <address>127.0.0.1:3210</address>\n"
            + "        </cluster-members>\n"
            + "    </network>\n"
            + "</hazelcast-client>\n";

    // Client configuration with port where there is no Hz 3 instance running
    protected static final String HZ3_DOWN_CLIENT_CONFIG =
            "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
            + "<hazelcast-client xmlns=\"http://www.hazelcast.com/schema/client-config\"\n"
            + "                  xmlns:xsi=\"http://www.w3.org/2001/XMLSchema-instance\"\n"
            + "                  xsi:schemaLocation=\"http://www.hazelcast.com/schema/client-config\n"
            + "                  http://www.hazelcast.com/schema/client-config/hazelcast-client-config-3.12.xsd\">\n"
            + "\n"
            + "    <network>\n"
            + "        <cluster-members>\n"
            + "            <address>127.0.0.1:42000</address>\n"
            + "        </cluster-members>\n"
            + "    </network>\n"
            + "</hazelcast-client>\n";

    protected HazelcastInstance hz3;
    protected Path customLibDir;


    @Before
    public void setUp() throws Exception {
        customLibDir = Files.createTempDirectory("source");
        System.setProperty(ClusterProperty.PROCESSOR_CUSTOM_LIB_DIR.getName(),
                customLibDir.toAbsolutePath().toString());

        Files.copy(
                getJarPath("com.hazelcast", "hazelcast", "3.12.12"),
                customLibDir.resolve("hazelcast-3.12.12.jar")
        );
        Files.copy(
                getJarPath("com.hazelcast", "hazelcast-client", "3.12.12"),
                customLibDir.resolve("hazelcast-client-3.12.12.jar")
        );
        File file = findConnectorImplJar();
        Files.copy(file.toPath().toAbsolutePath(), customLibDir.resolve("hazelcast-3-connector-impl.jar"));

        hz3 = Hazelcast3Starter.newHazelcastInstance(HZ3_MEMBER_CONFIG);
    }

    @Nonnull
    private Path getJarPath(String groupId, String artifactId, String version) {
        String home = System.getProperty("user.home");
        return Paths.get(home + "/.m2/repository/" +
                         groupId.replace(".", "/") + "/" +
                         artifactId + "/" +
                         version + "/" +
                         artifactId + "-" + version + ".jar"
        ).toAbsolutePath();
    }

    private File findConnectorImplJar() {
        File[] files = new File("../hazelcast-3-connector-impl/target/").listFiles();
        File file = Arrays.stream(files).filter(f -> f.getName().matches("hazelcast-3-connector-impl-.*-SNAPSHOT.jar"))
                .findFirst()
                .orElseThrow(() -> new RuntimeException("Could not find hazelcast-3-connector-impl jar, " +
                                                        "build the module using maven first"));
        return file;
    }

    @After
    public void tearDown() throws Exception {
        if (hz3 != null) {
            hz3.shutdown();
        }
        if (customLibDir != null) {
            customLibDir.toFile().delete();
        }
    }

    protected JobConfig getJobConfig(String name) {
        JobConfig config = new JobConfig();
        List<String> jars = new ArrayList<>();
        jars.add("hazelcast-3.12.12.jar");
        jars.add("hazelcast-client-3.12.12.jar");
        jars.add("hazelcast-3-connector-impl.jar");
        config.addCustomClasspaths(name, jars);
        return config;
    }


}
