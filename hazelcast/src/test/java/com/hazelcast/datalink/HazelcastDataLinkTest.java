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

package com.hazelcast.datalink;

import com.hazelcast.config.Config;
import com.hazelcast.config.DataLinkConfig;
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
public class HazelcastDataLinkTest extends HazelcastTestSupport {

    private String clusterName;
    private HazelcastInstance instance;
    private HazelcastDataLink hazelcastDataLink;

    @Before
    public void setUp() throws Exception {
        clusterName = randomName();
        instance = Hazelcast.newHazelcastInstance(new Config().setClusterName(clusterName));
    }

    @After
    public void tearDown() throws Exception {
        if (hazelcastDataLink != null) {
            hazelcastDataLink.release();
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

        DataLinkConfig dataLinkConfig = sharedDataLinkConfig(clusterName);

        hazelcastDataLink = new HazelcastDataLink(dataLinkConfig);
        Collection<DataLinkResource> resources = hazelcastDataLink.listResources();

        assertThat(resources).contains(new DataLinkResource("IMap", "my_map"));
    }

    @Test
    public void list_resources_should_not_return_system_maps() throws Exception {
        DataLinkConfig dataLinkConfig = sharedDataLinkConfig(clusterName);

        hazelcastDataLink = new HazelcastDataLink(dataLinkConfig);
        Collection<DataLinkResource> resources = hazelcastDataLink.listResources();

        assertThat(resources).isEmpty();
    }

    @Test
    public void should_throw_with_missing_config() {
        DataLinkConfig dataLinkConfig = new DataLinkConfig("data-link-name")
                .setType("hz")
                .setShared(true);

        assertThatThrownBy(() -> hazelcastDataLink = new HazelcastDataLink(dataLinkConfig))
                .isInstanceOf(HazelcastException.class)
                .hasMessage("HazelcastDataLink with name 'data-link-name' "
                        + "could not be created, provide either client_xml or client_yml property "
                        + "with the client configuration.");
    }

    @Test
    public void shared_client_should_return_same_instance() {
        DataLinkConfig dataLinkConfig = sharedDataLinkConfig(clusterName);
        hazelcastDataLink = new HazelcastDataLink(dataLinkConfig);
        HazelcastInstance c1 = hazelcastDataLink.getClient();
        HazelcastInstance c2 = hazelcastDataLink.getClient();

        try {
            assertThat(c1).isSameAs(c2);
        } finally {
            c1.shutdown();
            c2.shutdown();
        }
    }

    @Test
    public void non_shared_client_should_return_new_client_instance() {
        DataLinkConfig dataLinkConfig = nonSharedDataLinkConfig(clusterName);
        hazelcastDataLink = new HazelcastDataLink(dataLinkConfig);
        HazelcastInstance c1 = hazelcastDataLink.getClient();
        HazelcastInstance c2 = hazelcastDataLink.getClient();

        try {
            assertThat(c1).isNotSameAs(c2);
        } finally {
            c1.shutdown();
            c2.shutdown();
        }
    }

    private static DataLinkConfig nonSharedDataLinkConfig(String clusterName) {
        return sharedDataLinkConfig(clusterName)
                .setShared(false);
    }
    @Nonnull
    private static DataLinkConfig sharedDataLinkConfig(String clusterName) {
        DataLinkConfig dataLinkConfig = new DataLinkConfig("data-link-name")
                .setType("HZ")
                .setShared(true);
        try {
            byte[] bytes = readAllBytes(Paths.get("src", "test", "resources", "hazelcast-client-test-external.xml"));
            String xmlString = new String(bytes, StandardCharsets.UTF_8).replace("$CLUSTER_NAME$", clusterName);
            dataLinkConfig.setProperty(HazelcastDataLink.CLIENT_XML, xmlString);
            return dataLinkConfig;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
