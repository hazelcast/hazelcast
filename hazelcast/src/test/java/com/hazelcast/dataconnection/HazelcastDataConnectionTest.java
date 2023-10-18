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

package com.hazelcast.dataconnection;

import com.hazelcast.config.Config;
import com.hazelcast.config.DataConnectionConfig;
import com.hazelcast.core.Hazelcast;
import com.hazelcast.core.HazelcastException;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.IMap;
import com.hazelcast.test.HazelcastSerialClassRunner;
import com.hazelcast.test.HazelcastTestSupport;
import com.hazelcast.test.annotation.QuickTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import javax.annotation.Nonnull;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.Locale;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@RunWith(HazelcastSerialClassRunner.class)
@Category({QuickTest.class})
public class HazelcastDataConnectionTest extends HazelcastTestSupport {

    private String clusterName;
    private HazelcastInstance instance;
    private HazelcastDataConnection hazelcastDataConnection;

    @Before
    public void setUp() throws Exception {
        clusterName = randomName();
        instance = Hazelcast.newHazelcastInstance(new Config().setClusterName(clusterName));
    }

    @After
    public void tearDown() throws Exception {
        if (hazelcastDataConnection != null) {
            hazelcastDataConnection.release();
        }
        if (instance != null) {
            instance.shutdown();
            instance = null;
        }
    }

    @Test
    public void list_resources_should_return_map() {
        IMap<Integer, String> map = instance.getMap("my_map");
        map.put(42, "42");

        DataConnectionConfig dataConnectionConfig = sharedDataConnectionConfig(clusterName);

        hazelcastDataConnection = new HazelcastDataConnection(dataConnectionConfig);
        Collection<DataConnectionResource> resources = hazelcastDataConnection.listResources();

        assertThat(resources).contains(new DataConnectionResource("IMapJournal", "my_map"));
    }

    @Test
    public void list_resources_should_not_return_system_maps() {
        DataConnectionConfig dataConnectionConfig = sharedDataConnectionConfig(clusterName);

        hazelcastDataConnection = new HazelcastDataConnection(dataConnectionConfig);
        Collection<DataConnectionResource> resources = hazelcastDataConnection.listResources();

        assertThat(resources).isEmpty();
    }

    @Test
    public void should_throw_with_missing_config() {
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig("data-connection-name")
                .setType("hz")
                .setShared(true);

        assertThatThrownBy(() -> hazelcastDataConnection = new HazelcastDataConnection(dataConnectionConfig))
                .isInstanceOf(HazelcastException.class)
                .hasMessage("HazelcastDataConnection with name 'data-connection-name' "
                            + "could not be created, "
                            + "provide either a file path with one of "
                            + "\"client_xml_path\" or \"client_yml_path\" properties "
                            + "or a string content with one of \"client_xml\" or \"client_yml\" properties "
                            + "for the client configuration.");
    }

    @Test
    public void should_throw_with_empty_filepath() {
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig("data-link-name")
                .setType(HazelcastDataConnection.class.getName())
                .setProperty(HazelcastDataConnection.CLIENT_YML_PATH, "")
                .setShared(true);

        assertThatThrownBy(() -> hazelcastDataConnection = new HazelcastDataConnection(dataConnectionConfig))
                .isInstanceOf(HazelcastException.class);
    }

    @Test
    public void should_throw_with_filepath_string() {
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig("data-link-name")
                .setType(HazelcastDataConnection.class.getName())
                .setProperty(HazelcastDataConnection.CLIENT_YML_PATH, "")
                .setProperty(HazelcastDataConnection.CLIENT_YML, "")
                .setShared(true);

        assertThatThrownBy(() -> hazelcastDataConnection = new HazelcastDataConnection(dataConnectionConfig))
                .isInstanceOf(HazelcastException.class);
    }

    @Test
    public void shared_client_should_return_same_instance() {
        DataConnectionConfig dataConnectionConfig = sharedDataConnectionConfig(clusterName);
        hazelcastDataConnection = new HazelcastDataConnection(dataConnectionConfig);
        HazelcastInstance c1 = hazelcastDataConnection.getClient();
        HazelcastInstance c2 = hazelcastDataConnection.getClient();

        try {
            assertThat(c1).isSameAs(c2);
        } finally {
            c1.shutdown();
            c2.shutdown();
        }
    }

    @Test
    public void shared_client_from_file_should_return_same_instance() {
        DataConnectionConfig dataConnectionConfig = sharedDataConnectionConfigFromFile(clusterName);
        hazelcastDataConnection = new HazelcastDataConnection(dataConnectionConfig);
        HazelcastInstance c1 = hazelcastDataConnection.getClient();
        HazelcastInstance c2 = hazelcastDataConnection.getClient();

        try {
            assertThat(c1).isSameAs(c2);
        } finally {
            c1.shutdown();
            c2.shutdown();
            // Delete the file at the end of the test
            try {
                String filePath = dataConnectionConfig.getProperty(HazelcastDataConnection.CLIENT_XML_PATH);
                assert filePath != null;
                Files.delete(Paths.get(filePath));
            } catch (IOException ignored) {
            }
        }
    }

    @Test(timeout = 60_000)
    public void shared_client_should_be_initialized_lazy() {
        int invalidPort = 9999;
        DataConnectionConfig dataConnectionConfig = sharedDataConnectionConfig(clusterName, invalidPort);
        hazelcastDataConnection = new HazelcastDataConnection(dataConnectionConfig);
        assertThatNoException().isThrownBy(() -> new HazelcastDataConnection(dataConnectionConfig));
    }

    @Test
    public void non_shared_client_should_return_new_client_instance() {
        DataConnectionConfig dataConnectionConfig = nonSharedDataConnectionConfig(clusterName);
        hazelcastDataConnection = new HazelcastDataConnection(dataConnectionConfig);
        HazelcastInstance c1 = hazelcastDataConnection.getClient();
        HazelcastInstance c2 = hazelcastDataConnection.getClient();

        try {
            assertThat(c1).isNotSameAs(c2);
        } finally {
            c1.shutdown();
            c2.shutdown();
        }
    }

    @Test
    public void should_list_resource_types() {
        // given
        DataConnectionConfig dataConnectionConfig = nonSharedDataConnectionConfig(clusterName);
        hazelcastDataConnection = new HazelcastDataConnection(dataConnectionConfig);

        // when
        Collection<String> resourcedTypes = hazelcastDataConnection.resourceTypes();

        //then
        assertThat(resourcedTypes)
                .map(r -> r.toLowerCase(Locale.ROOT))
                .containsExactlyInAnyOrder("imapjournal");
    }

    private static DataConnectionConfig nonSharedDataConnectionConfig(String clusterName) {
        return sharedDataConnectionConfig(clusterName)
                .setShared(false);
    }

    @Nonnull
    private static DataConnectionConfig sharedDataConnectionConfig(String clusterName) {
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig("data-connection-name")
                .setType("HZ")
                .setShared(true);
        try {
            String str = readFile();
            String xmlString = str.replace("$CLUSTER_NAME$", clusterName);
            dataConnectionConfig.setProperty(HazelcastDataConnection.CLIENT_XML, xmlString);
            return dataConnectionConfig;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Nonnull
    private static DataConnectionConfig sharedDataConnectionConfig(String clusterName, int port) {
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig("data-connection-name")
                .setType("HZ")
                .setShared(true);
        try {
            String str = readFile();
            String xmlString = str
                    .replace("$CLUSTER_NAME$", clusterName)
                    .replace("5701", Integer.toString(port));
            dataConnectionConfig.setProperty(HazelcastDataConnection.CLIENT_XML, xmlString);
            return dataConnectionConfig;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Nonnull
    private static DataConnectionConfig sharedDataConnectionConfigFromFile(String clusterName) {
        DataConnectionConfig dataConnectionConfig = new DataConnectionConfig("data-link-name")
                .setType(HazelcastDataConnection.class.getName())
                .setShared(true);
        try {
            String str = readFile();
            String xmlString = str.replace("$CLUSTER_NAME$", clusterName);
            Path tempFile = Files.createTempFile("test_client", ".xml");
            Files.write(tempFile, xmlString.getBytes(StandardCharsets.UTF_8));
            dataConnectionConfig.setProperty(HazelcastDataConnection.CLIENT_XML_PATH, tempFile.toString());

            return dataConnectionConfig;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String readFile() throws IOException {
        return Files.readString(Paths.get("src", "test", "resources", "hazelcast-client-test-external.xml"));
    }
}
