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
import java.nio.file.Paths;
import java.util.Collection;

import static java.nio.file.Files.readAllBytes;
import static org.assertj.core.api.Assertions.assertThat;
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
    public void list_resources_should_return_map() throws Exception {
        IMap<Integer, String> map = instance.getMap("my_map");
        map.put(42, "42");

        DataConnectionConfig dataConnectionConfig = sharedDataConnectionConfig(clusterName);

        hazelcastDataConnection = new HazelcastDataConnection(dataConnectionConfig);
        Collection<DataConnectionResource> resources = hazelcastDataConnection.listResources();

        assertThat(resources).contains(new DataConnectionResource("IMap", "my_map"));
    }

    @Test
    public void list_resources_should_not_return_system_maps() throws Exception {
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
                        + "could not be created, provide either client_xml or client_yml property "
                        + "with the client configuration.");
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
            byte[] bytes = readAllBytes(Paths.get("src", "test", "resources", "hazelcast-client-test-external.xml"));
            String xmlString = new String(bytes, StandardCharsets.UTF_8).replace("$CLUSTER_NAME$", clusterName);
            dataConnectionConfig.setProperty(HazelcastDataConnection.CLIENT_XML, xmlString);
            return dataConnectionConfig;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
